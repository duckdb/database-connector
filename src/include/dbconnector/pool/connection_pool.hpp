#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>

extern "C" {
#include "duckdb.h"
}

#include "pooled_connection.hpp"
#include "thread_local_connection_cache.hpp"

namespace dbconnector {
namespace pool {

template <typename ConnectionT>
class ConnectionPool : public std::enable_shared_from_this<ConnectionPool<ConnectionT>> {
public:
	static constexpr idx_t DEFAULT_POOL_SIZE = 4;
	static constexpr idx_t DEFAULT_POOL_TIMEOUT_MS = 30000;

	ConnectionPool(idx_t max_connections = DEFAULT_POOL_SIZE, idx_t timeout_ms = DEFAULT_POOL_TIMEOUT_MS,
	               bool thread_local_cache_enabled = false);
	virtual ~ConnectionPool();

	PooledConnection<ConnectionT> Acquire();
	PooledConnection<ConnectionT> TryAcquire();
	PooledConnection<ConnectionT> ForceAcquire();

	void Return(std::unique_ptr<ConnectionT> conn);
	void Discard();
	void Shutdown();

	void SetMaxConnections(idx_t new_max);

	idx_t GetMaxConnections() const;
	idx_t GetAvailableConnections() const;
	idx_t GetTotalConnections() const;
	bool IsShutdown() const;

	idx_t GetThreadLocalCacheHits() const;
	idx_t GetThreadLocalCacheMisses() const;
	void SetThreadLocalCacheEnabled(bool enabled);
	bool IsThreadLocalCacheEnabled() const;

protected:
	virtual std::unique_ptr<ConnectionT> CreateNewConnection() = 0;
	virtual bool CheckConnectionHealthy(ConnectionT &conn) = 0;
	virtual void ResetConnection(ConnectionT &conn) = 0;
	virtual bool TryRecoverConnection(ConnectionT &conn) {
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

	std::unique_ptr<ConnectionT> TryAcquireFromThreadLocal();
	bool TryReturnToThreadLocal(std::unique_ptr<ConnectionT> &conn);
	void ReturnFromThreadLocalCache(std::unique_ptr<ConnectionT> conn);

	idx_t max_connections;
	idx_t timeout_ms;
	mutable std::mutex pool_lock;
	std::condition_variable pool_cv;
	std::deque<std::unique_ptr<ConnectionT>> available;
	idx_t total_connections = 0;
	bool shutdown_flag = false;

	std::atomic<bool> tl_cache_enabled;
	std::atomic<idx_t> tl_cache_hits {0};
	std::atomic<idx_t> tl_cache_misses {0};
};

} // namespace pool
} // namespace dbconnector
