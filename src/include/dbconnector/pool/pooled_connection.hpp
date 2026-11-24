#pragma once

#include <chrono>
#include <memory>

#include "dbconnector/pool/cached_connection.hpp"

namespace dbconnector {
namespace pool {

template <typename ConnectionT>
class ConnectionPool;

template <typename ConnectionT>
class PooledConnection {
public:
	PooledConnection();
	PooledConnection(std::shared_ptr<ConnectionPool<ConnectionT>> pool, std::unique_ptr<ConnectionT> connection,
	                 std::chrono::steady_clock::time_point created_at);
	PooledConnection(std::shared_ptr<ConnectionPool<ConnectionT>> pool, CachedConnection<ConnectionT> cached_conn);
	~PooledConnection() noexcept;

	//! A connection that is acquired from the pool when the pooling is disabled: it is closed instead of being
	//! returned, but can still be pinned in the pool it was acquired from
	static PooledConnection Unpooled(ConnectionPool<ConnectionT> &origin, std::unique_ptr<ConnectionT> connection,
	                                 std::chrono::steady_clock::time_point created_at);

	PooledConnection(const PooledConnection &) = delete;
	PooledConnection &operator=(const PooledConnection &) = delete;

	PooledConnection(PooledConnection &&other) noexcept;
	PooledConnection &operator=(PooledConnection &&other) noexcept;

	uint64_t Id();
	ConnectionT &GetConnection();
	ConnectionT *operator->();
	explicit operator bool() const;
	std::chrono::steady_clock::time_point GetCreatedAt();

	void Invalidate();
	bool OriginatesFrom(ConnectionPool<ConnectionT> *conn_pool);
	void PinBack();

private:
	void ReturnToPool() noexcept;

	uint64_t id = 0;
	std::shared_ptr<ConnectionPool<ConnectionT>> pool;
	//! The pool the connection was acquired from - also set for the unpooled connections (that have no pool)
	ConnectionPool<ConnectionT> *origin = nullptr;
	std::unique_ptr<ConnectionT> connection;
	bool valid = false;
	std::chrono::steady_clock::time_point created_at;

	static uint64_t NextId();
};

} // namespace pool
} // namespace dbconnector
