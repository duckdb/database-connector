#pragma once

#include <memory>

namespace dbconnector {
namespace pool {

template <typename ConnectionT>
class ConnectionPool;

template <typename ConnectionT>
class PooledConnection {
public:
	PooledConnection();
	PooledConnection(std::shared_ptr<ConnectionPool<ConnectionT>> pool, std::unique_ptr<ConnectionT> connection);
	~PooledConnection() noexcept;

	PooledConnection(const PooledConnection &) = delete;
	PooledConnection &operator=(const PooledConnection &) = delete;

	PooledConnection(PooledConnection &&other) noexcept;
	PooledConnection &operator=(PooledConnection &&other) noexcept;

	ConnectionT &GetConnection();
	ConnectionT *operator->();
	explicit operator bool() const;

	void Invalidate();

private:
	void ReturnToPool() noexcept;

	std::shared_ptr<ConnectionPool<ConnectionT>> pool;
	std::unique_ptr<ConnectionT> connection;
	bool valid = false;
};

} // namespace pool
} // namespace dbconnector
