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

	PooledConnection(const PooledConnection &) = delete;
	PooledConnection &operator=(const PooledConnection &) = delete;

	PooledConnection(PooledConnection &&other) noexcept;
	PooledConnection &operator=(PooledConnection &&other) noexcept;

	ConnectionT &GetConnection();
	ConnectionT *operator->();
	explicit operator bool() const;
	std::chrono::steady_clock::time_point GetCreatedAt();

	void Invalidate();

private:
	void ReturnToPool() noexcept;

	std::shared_ptr<ConnectionPool<ConnectionT>> pool;
	std::unique_ptr<ConnectionT> connection;
	bool valid = false;
	std::chrono::steady_clock::time_point created_at;
};

} // namespace pool
} // namespace dbconnector
