#pragma once

#include <memory>
#include <string>
#include <thread>

#include "connection_pool.hpp"
#include "pool_exception.hpp"

namespace dbconnector {
namespace pool {

template <typename ConnectionT>
ConnectionPool<ConnectionT>::ConnectionPool(idx_t max_connections_p, idx_t timeout_ms_p,
                                            bool thread_local_cache_enabled_p)
    : max_connections(max_connections_p), timeout_ms(timeout_ms_p), total_connections(0), shutdown_flag(false),
      tl_cache_enabled(thread_local_cache_enabled_p) {
}

template <typename ConnectionT>
ConnectionPool<ConnectionT>::~ConnectionPool() {
	Shutdown();
}

template <typename ConnectionT>
void ConnectionPool<ConnectionT>::Shutdown() {
	{
		std::lock_guard<std::mutex> lock(pool_lock);
		if (shutdown_flag) {
			return;
		}
		shutdown_flag = true;
		available.clear();
	}
	pool_cv.notify_all();
	//! Other threads' TL caches self-cleanup via ThreadLocalConnectionCache destructors
	//! (owner.lock() returns nullptr after pool destruction).
}

template <typename ConnectionT>
bool ConnectionPool<ConnectionT>::IsShutdown() const {
	std::lock_guard<std::mutex> lock(pool_lock);
	return shutdown_flag;
}

template <typename ConnectionT>
std::unique_ptr<ConnectionT> ConnectionPool<ConnectionT>::TryAcquireFromThreadLocal() {
	if (!tl_cache_enabled.load(std::memory_order_relaxed)) {
		return nullptr;
	}

	auto &cache = GetThreadLocalCache();
	auto cached_owner = cache.owner.lock();
	if (!cached_owner || cached_owner.get() != this) {
		if (!cached_owner && cache.connection) {
			cache.Clear();
		}
		tl_cache_misses.fetch_add(1, std::memory_order_relaxed);
		return nullptr;
	}

	if (!cache.available || !cache.connection) {
		tl_cache_misses.fetch_add(1, std::memory_order_relaxed);
		return nullptr;
	}

	if (!CheckConnectionHealthy(*cache.connection)) {
		cache.Clear();
		tl_cache_misses.fetch_add(1, std::memory_order_relaxed);
		return nullptr;
	}

	cache.available = false;
	tl_cache_hits.fetch_add(1, std::memory_order_relaxed);
	return std::move(cache.connection);
}

template <typename ConnectionT>
bool ConnectionPool<ConnectionT>::TryReturnToThreadLocal(std::unique_ptr<ConnectionT> &conn) {
	if (!tl_cache_enabled.load(std::memory_order_relaxed)) {
		return false;
	}

	auto &cache = GetThreadLocalCache();
	auto cached_owner = cache.owner.lock();
	if (cached_owner && cached_owner.get() != this) {
		return false;
	}

	if (cache.connection != nullptr) {
		return false;
	}

	std::lock_guard<std::mutex> lock(pool_lock);
	if (shutdown_flag) {
		return false;
	}
	if (total_connections >= max_connections && available.empty()) {
		return false;
	}
	cache.connection = std::move(conn);
	cache.owner = this->shared_from_this();
	cache.available = true;
	return true;
}

template <typename ConnectionT>
void ConnectionPool<ConnectionT>::ReturnFromThreadLocalCache(std::unique_ptr<ConnectionT> conn) {
	if (!conn) {
		return;
	}

	{
		std::lock_guard<std::mutex> lock(pool_lock);
		if (shutdown_flag) {
			if (total_connections > 0) {
				total_connections--;
			}
			return;
		}
		available.push_back(std::move(conn));
	}
	pool_cv.notify_one();
}

template <typename ConnectionT>
PooledConnection<ConnectionT> ConnectionPool<ConnectionT>::Acquire() {
	auto tl_conn = TryAcquireFromThreadLocal();
	if (tl_conn) {
		return PooledConnection<ConnectionT>(this->shared_from_this(), std::move(tl_conn));
	}

	std::unique_lock<std::mutex> lock(pool_lock);

	auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);

	while (true) {
		if (shutdown_flag) {
			throw PoolException("Connection pool has been shut down");
		}

		while (!available.empty()) {
			auto conn = std::move(available.front());
			available.pop_front();

			lock.unlock();
			bool healthy = CheckConnectionHealthy(*conn);
			lock.lock();

			if (shutdown_flag) {
				if (total_connections > 0) {
					total_connections--;
				}
				throw PoolException("Connection pool has been shut down");
			}

			if (healthy) {
				return PooledConnection<ConnectionT>(this->shared_from_this(), std::move(conn));
			}
			if (total_connections > 0) {
				total_connections--;
			}
		}

		if (total_connections < max_connections) {
			total_connections++;
			lock.unlock();

			try {
				auto conn = CreateNewConnection();
				return PooledConnection<ConnectionT>(this->shared_from_this(), std::move(conn));
			} catch (...) {
				lock.lock();
				if (total_connections > 0) {
					total_connections--;
				}
				pool_cv.notify_one();
				throw;
			}
		}

		if (pool_cv.wait_until(lock, deadline) == std::cv_status::timeout) {
			throw PoolException("Connection pool timeout: all " + std::to_string(max_connections) +
			                    " connections in use, waited " + std::to_string(timeout_ms) + "ms");
		}
	}
}

template <typename ConnectionT>
PooledConnection<ConnectionT> ConnectionPool<ConnectionT>::TryAcquire() {
	auto tl_conn = TryAcquireFromThreadLocal();
	if (tl_conn) {
		return PooledConnection<ConnectionT>(this->shared_from_this(), std::move(tl_conn));
	}

	std::unique_lock<std::mutex> lock(pool_lock);

	if (shutdown_flag) {
		return PooledConnection<ConnectionT>();
	}

	while (!available.empty()) {
		auto conn = std::move(available.front());
		available.pop_front();

		lock.unlock();
		bool healthy = CheckConnectionHealthy(*conn);
		lock.lock();

		if (shutdown_flag) {
			if (total_connections > 0) {
				total_connections--;
			}
			return PooledConnection<ConnectionT>();
		}

		if (healthy) {
			return PooledConnection<ConnectionT>(this->shared_from_this(), std::move(conn));
		}
		if (total_connections > 0) {
			total_connections--;
		}
	}

	if (total_connections < max_connections) {
		total_connections++;
		lock.unlock();

		try {
			auto conn = CreateNewConnection();
			return PooledConnection<ConnectionT>(this->shared_from_this(), std::move(conn));
		} catch (...) {
			lock.lock();
			if (total_connections > 0) {
				total_connections--;
			}
			pool_cv.notify_one();
			throw;
		}
	}

	return PooledConnection<ConnectionT>();
}

template <typename ConnectionT>
PooledConnection<ConnectionT> ConnectionPool<ConnectionT>::ForceAcquire() {
	auto tl_conn = TryAcquireFromThreadLocal();
	if (tl_conn) {
		return PooledConnection<ConnectionT>(this->shared_from_this(), std::move(tl_conn));
	}

	{
		std::unique_lock<std::mutex> lock(pool_lock);

		if (shutdown_flag) {
			throw PoolException("Connection pool has been shut down");
		}

		while (!available.empty()) {
			auto conn = std::move(available.front());
			available.pop_front();

			lock.unlock();
			bool healthy = CheckConnectionHealthy(*conn);
			lock.lock();

			if (shutdown_flag) {
				if (total_connections > 0) {
					total_connections--;
				}
				throw PoolException("Connection pool has been shut down");
			}

			if (healthy) {
				return PooledConnection<ConnectionT>(this->shared_from_this(), std::move(conn));
			}
			if (total_connections > 0) {
				total_connections--;
			}
		}

		total_connections++;
	}

	try {
		auto conn = CreateNewConnection();
		return PooledConnection<ConnectionT>(this->shared_from_this(), std::move(conn));
	} catch (...) {
		std::lock_guard<std::mutex> lock(pool_lock);
		if (total_connections > 0) {
			total_connections--;
		}
		pool_cv.notify_one();
		throw;
	}
}

template <typename ConnectionT>
void ConnectionPool<ConnectionT>::Return(std::unique_ptr<ConnectionT> conn) {
	if (!conn) {
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

	if (TryReturnToThreadLocal(conn)) {
		return;
	}

	{
		std::lock_guard<std::mutex> lock(pool_lock);
		if (shutdown_flag) {
			if (total_connections > 0) {
				total_connections--;
			}
			return;
		}
		if (total_connections > max_connections) {
			if (total_connections > 0) {
				total_connections--;
			}
			return;
		}
		available.push_back(std::move(conn));
	}
	pool_cv.notify_one();
}

template <typename ConnectionT>
void ConnectionPool<ConnectionT>::Discard() {
	{
		std::lock_guard<std::mutex> lock(pool_lock);
		if (total_connections > 0) {
			total_connections--;
		}
	}
	pool_cv.notify_one();
}

template <typename ConnectionT>
void ConnectionPool<ConnectionT>::SetMaxConnections(idx_t new_max) {
	std::deque<std::unique_ptr<ConnectionT>> to_evict;
	{
		std::lock_guard<std::mutex> lock(pool_lock);
		max_connections = new_max;
		while (!available.empty() && total_connections > max_connections) {
			to_evict.push_back(std::move(available.back()));
			available.pop_back();
			total_connections--;
		}
	}
	pool_cv.notify_all();
}

template <typename ConnectionT>
idx_t ConnectionPool<ConnectionT>::GetMaxConnections() const {
	std::lock_guard<std::mutex> lock(pool_lock);
	return max_connections;
}

template <typename ConnectionT>
idx_t ConnectionPool<ConnectionT>::GetAvailableConnections() const {
	std::lock_guard<std::mutex> lock(pool_lock);
	return available.size();
}

template <typename ConnectionT>
idx_t ConnectionPool<ConnectionT>::GetTotalConnections() const {
	std::lock_guard<std::mutex> lock(pool_lock);
	return total_connections;
}

template <typename ConnectionT>
idx_t ConnectionPool<ConnectionT>::GetThreadLocalCacheHits() const {
	return tl_cache_hits.load(std::memory_order_relaxed);
}

template <typename ConnectionT>
idx_t ConnectionPool<ConnectionT>::GetThreadLocalCacheMisses() const {
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
	for (auto &conn : available) {
		fn(*conn);
	}
}

} // namespace pool
} // namespace dbconnector
