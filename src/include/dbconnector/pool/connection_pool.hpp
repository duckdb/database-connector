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
	static constexpr uint64_t DEFAULT_POOL_SIZE = 4;
	static constexpr uint64_t DEFAULT_POOL_TIMEOUT_MS = 30000;

	enum class ThreadLocalCacheState {
		CACHE_ENABLED,
		CACHE_DISABLED,
	};

	ConnectionPool(uint64_t max_connections = DEFAULT_POOL_SIZE, uint64_t wait_timeout_millis = DEFAULT_POOL_TIMEOUT_MS,
	               ThreadLocalCacheState tl_cache_state = ThreadLocalCacheState::CACHE_DISABLED);
	virtual ~ConnectionPool();

	PooledConnection<ConnectionT> WaitAcquire();
	PooledConnection<ConnectionT> TryAcquire();
	PooledConnection<ConnectionT> ForceAcquire();

	bool IsShutdown() const;
	void Shutdown();

	uint64_t GetMaxConnections() const;
	void SetMaxConnections(uint64_t new_max);
	uint64_t GetWaitTimeoutMillis() const;
	void SetWaitTimeoutMillis(uint64_t timeout_millis);

	uint64_t GetAvailableConnections() const;
	uint64_t GetTotalConnections() const;

	bool IsThreadLocalCacheEnabled() const;
	void SetThreadLocalCacheEnabled(bool enabled);
	uint64_t GetThreadLocalCacheHits() const;
	uint64_t GetThreadLocalCacheMisses() const;

	uint64_t GetMaxLifetimeMillis() const;
	void SetMaxLifetimeMillis(uint64_t new_max_lifetime_millis);
	uint64_t GetIdleTimeoutMillis() const;
	void SetIdleTimeoutMillis(uint64_t new_idle_timeout_millis);

	bool EnsureReaperRunning();
	void ShutdownReaper();

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
	friend class PooledConnection<ConnectionT>;
	friend struct ThreadLocalConnectionCache<ConnectionT>;

	static ThreadLocalConnectionCache<ConnectionT> &GetThreadLocalCache() {
		static thread_local ThreadLocalConnectionCache<ConnectionT> cache;
		return cache;
	}

	void Return(std::unique_ptr<ConnectionT> conn, std::chrono::steady_clock::time_point created_at);
	void Discard();

	bool TimeoutEnabled() const;
	std::chrono::steady_clock::time_point GetNowForTimeoutPurposes();
	static bool TimePointExpired(std::chrono::steady_clock::time_point point, uint64_t timeout_millis,
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

	void DecrementTotalConnections();

	std::atomic<uint64_t> max_connections {0};
	std::atomic<uint64_t> wait_timeout_millis {0};

	mutable std::mutex pool_lock;
	std::condition_variable pool_cv;

	std::deque<CachedConnection<ConnectionT>> available;
	std::atomic<uint64_t> available_connections {0};
	std::atomic<uint64_t> total_connections {0};
	std::atomic<bool> shutdown_flag {false};

	std::atomic<uint64_t> max_lifetime_millis {0};
	std::atomic<uint64_t> idle_timeout_millis {0};

	std::thread reaper_thread;
	std::condition_variable reaper_cv;
	std::atomic<bool> reaper_shutdown_flag {false};

	std::atomic<bool> tl_cache_enabled {false};
	std::atomic<uint64_t> tl_cache_hits {0};
	std::atomic<uint64_t> tl_cache_misses {0};
};

} // namespace pool
} // namespace dbconnector
