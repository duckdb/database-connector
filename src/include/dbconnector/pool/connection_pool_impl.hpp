#pragma once

#include <memory>
#include <string>
#include <vector>

#include "dbconnector/defer.hpp"

#include "dbconnector/pool/connection_pool.hpp"
#include "dbconnector/pool/connection_pool_config.hpp"
#include "dbconnector/pool/pool_exception.hpp"

namespace dbconnector {
namespace pool {

template <typename ConnectionT>
ConnectionPool<ConnectionT>::ConnectionPool(ConnectionPoolConfig config)
    : max_connections(config.max_connections), wait_timeout_millis(config.wait_timeout_millis),
      max_lifetime_millis(config.max_lifetime_millis), idle_timeout_millis(config.idle_timeout_millis),
      tl_cache_enabled(config.tl_cache_enabled) {
	if (config.start_reaper_thread) {
		EnsureReaperRunning();
	}
}

template <typename ConnectionT>
ConnectionPool<ConnectionT>::~ConnectionPool() {
	Shutdown();
}

template <typename ConnectionT>
void ConnectionPool<ConnectionT>::Shutdown() {
	{
		std::unique_lock<std::mutex> lock(pool_lock);
		ShutdownReaperInternal(lock);
		if (shutdown_flag.load(std::memory_order_relaxed)) {
			return;
		}
		shutdown_flag.store(true, std::memory_order_relaxed);
		available.clear();
		available_connections.store(available.size(), std::memory_order_relaxed);
	}
	pool_cv.notify_all();
	//! Other threads' TL caches self-cleanup via ThreadLocalConnectionCache destructors
	//! (owner.lock() returns nullptr after pool destruction).
}

template <typename ConnectionT>
bool ConnectionPool<ConnectionT>::IsShutdown() const {
	return shutdown_flag.load(std::memory_order_relaxed);
}

template <typename ConnectionT>
CachedConnection<ConnectionT>
ConnectionPool<ConnectionT>::TryAcquireFromThreadLocal(std::chrono::steady_clock::time_point now) {
	if (!tl_cache_enabled.load(std::memory_order_relaxed)) {
		return CachedConnection<ConnectionT>();
	}

	auto &cache = GetThreadLocalCache();
	auto cached_owner = cache.owner.lock();
	if (!cached_owner || cached_owner.get() != this) {
		if (!cached_owner && cache.cached_conn) {
			cache.Clear();
		}
		tl_cache_misses.fetch_add(1, std::memory_order_relaxed);
		return CachedConnection<ConnectionT>();
	}

	if (!cache.available || !cache.cached_conn) {
		tl_cache_misses.fetch_add(1, std::memory_order_relaxed);
		return CachedConnection<ConnectionT>();
	}

	if (IsExpired(cache.cached_conn, now)) {
		cache.Clear();
		tl_cache_misses.fetch_add(1, std::memory_order_relaxed);
		return CachedConnection<ConnectionT>();
	}

	if (!CheckConnectionHealthy(cache.cached_conn.GetConnection())) {
		cache.Clear();
		tl_cache_misses.fetch_add(1, std::memory_order_relaxed);
		return CachedConnection<ConnectionT>();
	}

	cache.available = false;
	tl_cache_hits.fetch_add(1, std::memory_order_relaxed);
	return std::move(cache.cached_conn);
}

template <typename ConnectionT>
bool ConnectionPool<ConnectionT>::TryReturnToThreadLocal(std::unique_ptr<ConnectionT> &conn,
                                                         std::chrono::steady_clock::time_point created_at,
                                                         std::chrono::steady_clock::time_point returned_at) {
	if (!tl_cache_enabled.load(std::memory_order_relaxed)) {
		return false;
	}

	auto &cache = GetThreadLocalCache();
	auto cached_owner = cache.owner.lock();
	if (cached_owner && cached_owner.get() != this) {
		return false;
	}

	if (cache.cached_conn) {
		return false;
	}

	std::lock_guard<std::mutex> lock(pool_lock);
	if (shutdown_flag.load(std::memory_order_relaxed)) {
		return false;
	}
	if (total_connections.load(std::memory_order_relaxed) >= max_connections.load(std::memory_order_relaxed) &&
	    available.empty()) {
		return false;
	}
	cache.cached_conn = CachedConnection<ConnectionT>(std::move(conn), created_at, returned_at);
	cache.owner = this->shared_from_this();
	cache.available = true;
	return true;
}

template <typename ConnectionT>
void ConnectionPool<ConnectionT>::ReturnFromThreadLocalCache(CachedConnection<ConnectionT> cached_conn) {
	if (!cached_conn) {
		return;
	}

	auto now = GetNowForTimeoutPurposes();
	bool expired = IsExpired(cached_conn, now);

	{
		std::lock_guard<std::mutex> lock(pool_lock);
		if (expired || shutdown_flag.load(std::memory_order_relaxed)) {
			DecrementTotalConnections();
			return;
		}
		available.emplace_back(std::move(cached_conn));
		available_connections.store(available.size(), std::memory_order_relaxed);
	}
	pool_cv.notify_one();
}

template <typename ConnectionT>
PooledConnection<ConnectionT> ConnectionPool<ConnectionT>::WaitAcquire() {
	{
		std::lock_guard<std::mutex> lock(pool_lock);
		if (max_connections.load(std::memory_order_relaxed) == 0) {
			throw PoolException("Connection pool is disabled (pool_size=0). Use "
			                    "the force acquire mode to create connections without pooling.");
		}
	}

	auto now = GetNowForTimeoutPurposes();
	auto tl_conn = TryAcquireFromThreadLocal(now);
	if (tl_conn) {
		return PooledConnection<ConnectionT>(this->shared_from_this(), std::move(tl_conn));
	}

	std::unique_lock<std::mutex> lock(pool_lock);

	auto deadline = std::chrono::steady_clock::now() +
	                std::chrono::milliseconds(wait_timeout_millis.load(std::memory_order_relaxed));

	while (true) {
		if (shutdown_flag.load(std::memory_order_relaxed)) {
			throw PoolException("Connection pool has been shut down");
		}

		while (!available.empty()) {
			auto cached_conn = std::move(available.front());
			available.pop_front();
			available_connections.store(available.size(), std::memory_order_relaxed);

			now = GetNowForTimeoutPurposes();
			bool healthy = CheckConnectionNotExpiredAndHealthy(lock, cached_conn, now);

			if (shutdown_flag.load(std::memory_order_relaxed)) {
				DecrementTotalConnections();
				throw PoolException("Connection pool has been shut down");
			}

			if (healthy) {
				return PooledConnection<ConnectionT>(this->shared_from_this(), std::move(cached_conn));
			}
			DecrementTotalConnections();
		}

		if (total_connections.load(std::memory_order_relaxed) < max_connections.load(std::memory_order_relaxed)) {
			total_connections.fetch_add(1, std::memory_order_relaxed);
			lock.unlock();

			try {
				auto conn = CreateNewConnection();
				return PooledConnection<ConnectionT>(this->shared_from_this(), std::move(conn), now);
			} catch (...) {
				lock.lock();
				DecrementTotalConnections();
				pool_cv.notify_one();
				throw;
			}
		}

		// Spurious wakeups re-evaluate all conditions above. The deadline is not reset.
		if (pool_cv.wait_until(lock, deadline) == std::cv_status::timeout) {
			throw PoolException("Connection pool timeout: all " +
			                    std::to_string(max_connections.load(std::memory_order_relaxed)) +
			                    " connections in use, waited " +
			                    std::to_string(wait_timeout_millis.load(std::memory_order_relaxed)) + "ms");
		}
	}
}

template <typename ConnectionT>
PooledConnection<ConnectionT> ConnectionPool<ConnectionT>::TryAcquire() {
	{
		std::lock_guard<std::mutex> lock(pool_lock);
		if (max_connections.load(std::memory_order_relaxed) == 0) {
			throw PoolException("Connection pool is disabled (pool_size=0). Use "
			                    "the force acquire mode to create connections without pooling.");
		}
	}

	auto now = GetNowForTimeoutPurposes();

	auto tl_conn = TryAcquireFromThreadLocal(now);
	if (tl_conn) {
		return PooledConnection<ConnectionT>(this->shared_from_this(), std::move(tl_conn));
	}

	std::unique_lock<std::mutex> lock(pool_lock);

	if (shutdown_flag.load(std::memory_order_relaxed)) {
		return PooledConnection<ConnectionT>();
	}

	while (!available.empty()) {
		auto cached_conn = std::move(available.front());
		available.pop_front();
		available_connections.store(available.size(), std::memory_order_relaxed);

		bool healthy = CheckConnectionNotExpiredAndHealthy(lock, cached_conn, now);

		if (shutdown_flag.load(std::memory_order_relaxed)) {
			DecrementTotalConnections();
			return PooledConnection<ConnectionT>();
		}

		if (healthy) {
			return PooledConnection<ConnectionT>(this->shared_from_this(), std::move(cached_conn));
		}
		DecrementTotalConnections();
	}

	if (total_connections.load(std::memory_order_relaxed) < max_connections.load(std::memory_order_relaxed)) {
		total_connections.fetch_add(1, std::memory_order_relaxed);
		lock.unlock();

		try {
			auto conn = CreateNewConnection();
			return PooledConnection<ConnectionT>(this->shared_from_this(), std::move(conn), now);
		} catch (...) {
			lock.lock();
			DecrementTotalConnections();
			pool_cv.notify_one();
			throw;
		}
	}

	return PooledConnection<ConnectionT>();
}

template <typename ConnectionT>
PooledConnection<ConnectionT> ConnectionPool<ConnectionT>::ForceAcquire() {
	auto now = GetNowForTimeoutPurposes();

	// We return the thread-local connection even if the running pool was disabled
	// by setting max_conn = 0.
	auto tl_conn = TryAcquireFromThreadLocal(now);
	if (tl_conn) {
		return PooledConnection<ConnectionT>(this->shared_from_this(), std::move(tl_conn));
	}

	bool pooling_disabled = false;
	{
		std::lock_guard<std::mutex> lock(pool_lock);
		if (shutdown_flag.load(std::memory_order_relaxed)) {
			throw PoolException("Connection pool has been shut down");
		}
		pooling_disabled = (max_connections.load(std::memory_order_relaxed) == 0);
	}

	if (pooling_disabled) {
		auto conn = CreateNewConnection();
		return PooledConnection<ConnectionT>(nullptr, std::move(conn), now);
	}

	{
		std::unique_lock<std::mutex> lock(pool_lock);

		if (shutdown_flag.load(std::memory_order_relaxed)) {
			throw PoolException("Connection pool has been shut down");
		}

		while (!available.empty()) {
			auto cached_conn = std::move(available.front());
			available.pop_front();
			available_connections.store(available.size(), std::memory_order_relaxed);

			bool healthy = CheckConnectionNotExpiredAndHealthy(lock, cached_conn, now);

			if (shutdown_flag.load(std::memory_order_relaxed)) {
				DecrementTotalConnections();
				throw PoolException("Connection pool has been shut down");
			}

			if (healthy) {
				return PooledConnection<ConnectionT>(this->shared_from_this(), std::move(cached_conn));
			}
			DecrementTotalConnections();
		}

		total_connections.fetch_add(1, std::memory_order_relaxed);
	}

	try {
		auto conn = CreateNewConnection();
		return PooledConnection<ConnectionT>(this->shared_from_this(), std::move(conn), now);
	} catch (...) {
		std::lock_guard<std::mutex> lock(pool_lock);
		DecrementTotalConnections();
		pool_cv.notify_one();
		throw;
	}
}

template <typename ConnectionT>
void ConnectionPool<ConnectionT>::Return(std::unique_ptr<ConnectionT> conn,
                                         std::chrono::steady_clock::time_point created_at) {
	if (!conn) {
		return;
	}

	auto now = GetNowForTimeoutPurposes();

	if (IsExpired(created_at, now)) {
		Discard();
		return;
	}

	if (!CheckConnectionHealthy(*conn)) {
		if (!TryRecoverConnection(*conn)) {
			Discard();
			return;
		}
	}

	try {
		ResetConnection(*conn);
	} catch (...) {
		Discard();
		return;
	}

	if (TryReturnToThreadLocal(conn, created_at, now)) {
		return;
	}

	{
		std::lock_guard<std::mutex> lock(pool_lock);
		if (shutdown_flag.load(std::memory_order_relaxed)) {
			DecrementTotalConnections();
			return;
		}
		if (total_connections.load(std::memory_order_relaxed) > max_connections.load(std::memory_order_relaxed)) {
			DecrementTotalConnections();
			return;
		}
		CachedConnection<ConnectionT> cached_conn(std::move(conn), created_at, now);
		available.emplace_back(std::move(cached_conn));
		available_connections.store(available.size(), std::memory_order_relaxed);
	}
	pool_cv.notify_one();
}

template <typename ConnectionT>
void ConnectionPool<ConnectionT>::Discard() {
	{
		std::lock_guard<std::mutex> lock(pool_lock);
		DecrementTotalConnections();
	}
	pool_cv.notify_one();
}

template <typename ConnectionT>
uint64_t ConnectionPool<ConnectionT>::GetMaxConnections() const {
	return max_connections.load(std::memory_order_relaxed);
}

template <typename ConnectionT>
void ConnectionPool<ConnectionT>::SetMaxConnections(uint64_t new_max) {
	std::deque<CachedConnection<ConnectionT>> to_evict;
	{
		std::lock_guard<std::mutex> lock(pool_lock);
		this->max_connections.store(new_max, std::memory_order_relaxed);
		while (!available.empty() &&
		       total_connections.load(std::memory_order_relaxed) > max_connections.load(std::memory_order_relaxed)) {
			to_evict.push_back(std::move(available.back()));
			available.pop_back();
			available_connections.store(available.size(), std::memory_order_relaxed);
			DecrementTotalConnections();
		}
	}
	pool_cv.notify_all();
}

template <typename ConnectionT>
uint64_t ConnectionPool<ConnectionT>::GetWaitTimeoutMillis() const {
	return wait_timeout_millis.load(std::memory_order_relaxed);
}

template <typename ConnectionT>
void ConnectionPool<ConnectionT>::SetWaitTimeoutMillis(uint64_t timeout_millis) {
	std::lock_guard<std::mutex> lock(pool_lock);
	wait_timeout_millis.store(timeout_millis, std::memory_order_relaxed);
}

template <typename ConnectionT>
uint64_t ConnectionPool<ConnectionT>::GetAvailableConnections() const {
	return available_connections.load(std::memory_order_relaxed);
}

template <typename ConnectionT>
uint64_t ConnectionPool<ConnectionT>::GetTotalConnections() const {
	return total_connections.load(std::memory_order_relaxed);
}

template <typename ConnectionT>
void ConnectionPool<ConnectionT>::SetMaxLifetimeMillis(uint64_t new_max_lifetime_millis) {
	this->max_lifetime_millis.store(new_max_lifetime_millis, std::memory_order_relaxed);
	reaper_cv.notify_all();
}

template <typename ConnectionT>
uint64_t ConnectionPool<ConnectionT>::GetMaxLifetimeMillis() const {
	return max_lifetime_millis.load(std::memory_order_relaxed);
}

template <typename ConnectionT>
void ConnectionPool<ConnectionT>::SetIdleTimeoutMillis(uint64_t new_idle_timeout_millis) {
	this->idle_timeout_millis.store(new_idle_timeout_millis, std::memory_order_relaxed);
	reaper_cv.notify_all();
}

template <typename ConnectionT>
uint64_t ConnectionPool<ConnectionT>::GetIdleTimeoutMillis() const {
	return idle_timeout_millis.load(std::memory_order_relaxed);
}

template <typename ConnectionT>
bool ConnectionPool<ConnectionT>::TimeoutEnabled() const {
	return max_lifetime_millis.load(std::memory_order_relaxed) > 0 ||
	       idle_timeout_millis.load(std::memory_order_relaxed) > 0;
}

template <typename ConnectionT>
std::chrono::steady_clock::time_point ConnectionPool<ConnectionT>::GetNowForTimeoutPurposes() {
	if (TimeoutEnabled()) {
		return std::chrono::steady_clock::now();
	} else {
		return std::chrono::steady_clock::time_point();
	}
}

template <typename ConnectionT>
bool ConnectionPool<ConnectionT>::TimePointExpired(std::chrono::steady_clock::time_point point, uint64_t timeout_millis,
                                                   std::chrono::steady_clock::time_point now) {
	if (now.time_since_epoch() == std::chrono::steady_clock::duration::zero()) {
		return false;
	}
	if (timeout_millis == 0) {
		return false;
	}
	int64_t age_signed_millis = std::chrono::duration_cast<std::chrono::milliseconds>(now - point).count();
	uint64_t age_millis = age_signed_millis > 0 ? static_cast<uint64_t>(age_signed_millis) : 0;
	return age_millis >= timeout_millis;
}

template <typename ConnectionT>
bool ConnectionPool<ConnectionT>::IsExpired(const CachedConnection<ConnectionT> &cached_conn,
                                            std::chrono::steady_clock::time_point now) const {
	if (now.time_since_epoch() == std::chrono::steady_clock::duration::zero()) {
		return false;
	}
	uint64_t max_lifetime_val = max_lifetime_millis.load(std::memory_order_relaxed);
	if (TimePointExpired(cached_conn.GetCreatedAt(), max_lifetime_val, now)) {
		return true;
	}
	uint64_t idle_timeout_val = idle_timeout_millis.load(std::memory_order_relaxed);
	if (TimePointExpired(cached_conn.GetReturnedAt(), idle_timeout_val, now)) {
		return true;
	}
	return false;
}

template <typename ConnectionT>
bool ConnectionPool<ConnectionT>::IsExpired(std::chrono::steady_clock::time_point created_at,
                                            std::chrono::steady_clock::time_point now) const {
	if (now.time_since_epoch() == std::chrono::steady_clock::duration::zero()) {
		return false;
	}
	uint64_t max_lifetime_val = max_lifetime_millis.load(std::memory_order_relaxed);
	return TimePointExpired(created_at, max_lifetime_val, now);
}

template <typename ConnectionT> // specified lock must be held
bool ConnectionPool<ConnectionT>::CheckConnectionNotExpiredAndHealthy(std::unique_lock<std::mutex> &lock,
                                                                      CachedConnection<ConnectionT> &cached_conn,
                                                                      std::chrono::steady_clock::time_point now) {
	if (IsExpired(cached_conn, now)) {
		return false;
	}

	lock.unlock();
	auto deferred = Defer([&lock] { lock.lock(); });

	return CheckConnectionHealthy(cached_conn.GetConnection());
}

template <typename ConnectionT>
uint64_t ConnectionPool<ConnectionT>::CalcReaperSleepSeconds() {
	uint64_t sleep_seconds = 30;
	uint64_t max_lifetime_seconds = max_lifetime_millis.load(std::memory_order_relaxed) / 1000;
	uint64_t idle_timeout_seconds = idle_timeout_millis.load(std::memory_order_relaxed) / 1000;

	if (max_lifetime_seconds > 0 && idle_timeout_seconds > 0) {
		sleep_seconds = (std::min)(max_lifetime_seconds, idle_timeout_seconds);
	} else if (max_lifetime_seconds > 0) {
		sleep_seconds = max_lifetime_seconds;
	} else if (idle_timeout_seconds > 0) {
		sleep_seconds = idle_timeout_seconds;
	}

	sleep_seconds = (std::max<uint64_t>)(1, sleep_seconds / 2);
	sleep_seconds = (std::min<uint64_t>)(60, sleep_seconds);

	return sleep_seconds;
}

template <typename ConnectionT>
void ConnectionPool<ConnectionT>::ReaperLoop() {
	std::unique_lock<std::mutex> lock(pool_lock);
	while (!reaper_shutdown_flag.load(std::memory_order_acquire)) {
		uint64_t sleep_seconds = CalcReaperSleepSeconds();
		reaper_cv.wait_for(lock, std::chrono::seconds(sleep_seconds),
		                   [this]() { return reaper_shutdown_flag.load(std::memory_order_acquire); });

		uint64_t max_lifetime_val = max_lifetime_millis.load(std::memory_order_relaxed);
		uint64_t idle_timeout_val = idle_timeout_millis.load(std::memory_order_relaxed);

		if (max_lifetime_val == 0 && idle_timeout_val == 0) {
			reaper_shutdown_flag.store(true, std::memory_order_release);
			break;
		}

		auto now = std::chrono::steady_clock::now();

		std::vector<CachedConnection<ConnectionT>> expired;
		for (auto it = available.begin(); it != available.end();) {
			auto &cached_conn = *it;
			if (TimePointExpired(cached_conn.GetCreatedAt(), max_lifetime_val, now) ||
			    TimePointExpired(cached_conn.GetReturnedAt(), idle_timeout_val, now)) {
				expired.emplace_back(std::move(cached_conn));
				it = available.erase(it); // erase returns iterator to next element
				DecrementTotalConnections();
			} else {
				++it;
			}
		}
		available_connections.store(available.size(), std::memory_order_relaxed);

		if (expired.size() > 0) {
			// release lock while destroying expired connections
			lock.unlock();
			auto deferred = Defer([&lock] { lock.lock(); });
			pool_cv.notify_all();
			expired.clear();
		}
	}
}

template <typename ConnectionT>
bool ConnectionPool<ConnectionT>::IsReaperRunning() {
	return !reaper_shutdown_flag.load(std::memory_order_relaxed);
}

template <typename ConnectionT>
bool ConnectionPool<ConnectionT>::EnsureReaperRunning() {
	if (!TimeoutEnabled()) {
		return false;
	}

	std::unique_lock<std::mutex> lock(pool_lock);

	if (shutdown_flag.load(std::memory_order_relaxed)) {
		return false;
	}

	if (reaper_thread.joinable()) {
		if (!reaper_shutdown_flag.load(std::memory_order_acquire)) {
			return true;
		}
		// There could be a situation where the reaper is dead but is
		// perceived to be running. If the reaper_shutdown_flag = true
		// and the thread is joinable then we could avoid any issue by
		// joining the dead thread first before starting a new one
		{
			lock.unlock();
			auto deferred = Defer([&lock] { lock.lock(); });
			reaper_cv.notify_all();
			reaper_thread.join();
		}
		// Re-checking the state after re-acquiring the lock
		if (reaper_thread.joinable()) {
			// The reaper was just restarted from another thread
			return true;
		}
	}

	reaper_shutdown_flag.store(false, std::memory_order_release);
	this->reaper_thread = std::thread(&ConnectionPool<ConnectionT>::ReaperLoop, this);
	return true;
}

template <typename ConnectionT>
void ConnectionPool<ConnectionT>::ShutdownReaperInternal(std::unique_lock<std::mutex> &lock) {
	if (!reaper_thread.joinable()) {
		return;
	}
	reaper_shutdown_flag.store(true, std::memory_order_release);
	lock.unlock();
	auto deferred = Defer([&lock] { lock.lock(); });
	reaper_cv.notify_all();
	reaper_thread.join();
}

template <typename ConnectionT>
void ConnectionPool<ConnectionT>::ShutdownReaper() {
	std::unique_lock<std::mutex> lock(pool_lock);
	ShutdownReaperInternal(lock);
}

template <typename ConnectionT>
uint64_t ConnectionPool<ConnectionT>::GetThreadLocalCacheHits() const {
	return tl_cache_hits.load(std::memory_order_relaxed);
}

template <typename ConnectionT>
uint64_t ConnectionPool<ConnectionT>::GetThreadLocalCacheMisses() const {
	return tl_cache_misses.load(std::memory_order_relaxed);
}

template <typename ConnectionT>
void ConnectionPool<ConnectionT>::SetThreadLocalCacheEnabled(bool enabled) {
	tl_cache_enabled.store(enabled, std::memory_order_relaxed);
}

template <typename ConnectionT>
bool ConnectionPool<ConnectionT>::IsThreadLocalCacheEnabled() const {
	return tl_cache_enabled.load(std::memory_order_relaxed);
}

template <typename ConnectionT>
template <typename Fn>
void ConnectionPool<ConnectionT>::ForEachIdleConnection(Fn &&fn) {
	std::lock_guard<std::mutex> lock(pool_lock);
	for (auto &cached_conn : available) {
		fn(cached_conn.GetConnection());
	}
}

template <typename ConnectionT>
void ConnectionPool<ConnectionT>::DecrementTotalConnections() {
	// Note, this function is only called when holding the lock
	// it is not supposed to be atomic.
	if (total_connections.load(std::memory_order_relaxed) > 0) {
		total_connections.fetch_sub(1, std::memory_order_relaxed);
	}
}

} // namespace pool
} // namespace dbconnector
