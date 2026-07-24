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

	uint64_t Id();
	ConnectionT &GetConnection();
	ConnectionT *operator->();
	explicit operator bool() const;
	std::chrono::steady_clock::time_point GetCreatedAt();

	void Invalidate();

private:
	void ReturnToPool() noexcept;

	uint64_t id = 0;
	std::shared_ptr<ConnectionPool<ConnectionT>> pool;
	std::unique_ptr<ConnectionT> connection;
	bool valid = false;
	std::chrono::steady_clock::time_point created_at;

	static uint64_t NextId();
};

} // namespace pool
} // namespace dbconnector
