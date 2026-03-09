#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>

#include "dbconnector/pool/cached_connection.hpp"
#include "dbconnector/pool/pooled_connection.hpp"
#include "dbconnector/pool/thread_local_connection_cache.hpp"

namespace dbconnector {
namespace pool {

template <typename ConnectionT>
class ConnectionPool : public std::enable_shared_from_this<ConnectionPool<ConnectionT>> {
public:
	static constexpr size_t DEFAULT_POOL_SIZE = 4;
	static constexpr size_t DEFAULT_POOL_TIMEOUT_MS = 30000;

	enum class ThreadLocalCacheState {
		CACHE_ENABLED,
		CACHE_DISABLED,
	};

	ConnectionPool(size_t max_connections = DEFAULT_POOL_SIZE, size_t timeout_ms = DEFAULT_POOL_TIMEOUT_MS,
	               ThreadLocalCacheState tl_cache_state = ThreadLocalCacheState::CACHE_DISABLED);
	virtual ~ConnectionPool();

	PooledConnection<ConnectionT> Acquire();
	PooledConnection<ConnectionT> TryAcquire();
	PooledConnection<ConnectionT> ForceAcquire();

	void Return(std::unique_ptr<ConnectionT> conn, std::chrono::steady_clock::time_point created_at);
	void Discard();
	void Shutdown();

	void SetMaxConnections(size_t new_max);
	size_t GetMaxConnections() const;
	size_t GetAvailableConnections() const;
	size_t GetTotalConnections() const;
	bool IsShutdown() const;

	void SetMaxLifetimeSeconds(uint64_t new_max_lifetime_seconds);
	uint64_t GetMaxLifetimeSeconds() const;
	void SetIdleTimeoutSeconds(uint64_t new_idle_timeout_seconds);
	uint64_t GetIdleTimeoutSeconds() const;

	bool EnsureReaperRunning();
	void ShutdownReaper();

	size_t GetThreadLocalCacheHits() const;
	size_t GetThreadLocalCacheMisses() const;
	void SetThreadLocalCacheEnabled(bool enabled);
	bool IsThreadLocalCacheEnabled() const;

protected:
	virtual std::unique_ptr<ConnectionT> CreateNewConnection() = 0;
	virtual bool CheckConnectionHealthy(ConnectionT &conn) = 0;
	virtual void ResetConnection(ConnectionT &conn) = 0;
	virtual bool TryRecoverConnection(ConnectionT &) {
		return false;
	}

	//! Calls fn(conn) for each idle connection while holding pool_lock.
	//! fn MUST NOT call any pool method that acquires pool_lock.
	template <typename Fn>
	void ForEachIdleConnection(Fn &&fn);

private:
	friend struct ThreadLocalConnectionCache<ConnectionT>;

	static ThreadLocalConnectionCache<ConnectionT> &GetThreadLocalCache() {
		static thread_local ThreadLocalConnectionCache<ConnectionT> cache;
		return cache;
	}

	bool TimeoutEnabled() const;
	std::chrono::steady_clock::time_point GetNowForTimeoutPurposes();
	static bool TimePointExpired(std::chrono::steady_clock::time_point point, uint64_t timeout,
	                             std::chrono::steady_clock::time_point now);
	bool IsExpired(const CachedConnection<ConnectionT> &cached_conn, std::chrono::steady_clock::time_point now) const;
	bool IsExpired(std::chrono::steady_clock::time_point created_at, std::chrono::steady_clock::time_point now) const;
	bool CheckConnectionNotExpiredAndHealthy(std::unique_lock<std::mutex> &lock,
	                                         CachedConnection<ConnectionT> &cached_conn,
	                                         std::chrono::steady_clock::time_point now);
	uint64_t CalcReaperSleepSeconds();
	void ReaperLoop();
	void ShutdownReaperInternal(std::unique_lock<std::mutex> &lock);

	CachedConnection<ConnectionT> TryAcquireFromThreadLocal(std::chrono::steady_clock::time_point now);
	bool TryReturnToThreadLocal(std::unique_ptr<ConnectionT> &conn, std::chrono::steady_clock::time_point created_at,
	                            std::chrono::steady_clock::time_point returned_at);
	void ReturnFromThreadLocalCache(CachedConnection<ConnectionT> cached_conn);

	size_t max_connections = 0;
	size_t timeout_ms = 0;

	mutable std::mutex pool_lock;
	std::condition_variable pool_cv;

	std::deque<CachedConnection<ConnectionT>> available;
	size_t total_connections = 0;
	bool shutdown_flag = false;

	std::atomic<uint64_t> max_lifetime_seconds {0};
	std::atomic<uint64_t> idle_timeout_seconds {0};

	std::thread reaper_thread;
	std::condition_variable reaper_cv;
	std::atomic<bool> reaper_shutdown_flag {false};

	std::atomic<bool> tl_cache_enabled;
	std::atomic<size_t> tl_cache_hits {0};
	std::atomic<size_t> tl_cache_misses {0};
};

} // namespace pool
} // namespace dbconnector
