#pragma once

#include "pooled_connection.hpp"

#include "connection_pool.hpp"
#include "pool_exception.hpp"

namespace dbconnector {
namespace pool {

template <typename ConnectionT>
PooledConnection<ConnectionT>::PooledConnection() : pool(nullptr), connection(nullptr), valid(false) {
}

template <typename ConnectionT>
PooledConnection<ConnectionT>::PooledConnection(std::shared_ptr<ConnectionPool<ConnectionT>> pool_p,
                                                std::unique_ptr<ConnectionT> connection_p)
    : pool(std::move(pool_p)), connection(std::move(connection_p)), valid(true) {
}

template <typename ConnectionT>
PooledConnection<ConnectionT>::~PooledConnection() noexcept {
	ReturnToPool();
}

template <typename ConnectionT>
PooledConnection<ConnectionT>::PooledConnection(PooledConnection &&other) noexcept
    : pool(std::move(other.pool)), connection(std::move(other.connection)), valid(other.valid) {
	other.valid = false;
}

template <typename ConnectionT>
PooledConnection<ConnectionT> &PooledConnection<ConnectionT>::operator=(PooledConnection &&other) noexcept {
	if (this != &other) {
		ReturnToPool();
		pool = std::move(other.pool);
		connection = std::move(other.connection);
		valid = other.valid;
		other.valid = false;
	}
	return *this;
}

template <typename ConnectionT>
ConnectionT &PooledConnection<ConnectionT>::GetConnection() {
	if (!connection) {
		throw PoolException("PooledConnection::GetConnection - no connection available");
	}
	return *connection;
}

template <typename ConnectionT>
ConnectionT *PooledConnection<ConnectionT>::operator->() {
	if (!connection) {
		throw PoolException("PooledConnection::operator-> - no connection available");
	}
	return connection.get();
}

template <typename ConnectionT>
PooledConnection<ConnectionT>::operator bool() const {
	return connection != nullptr && valid;
}

template <typename ConnectionT>
void PooledConnection<ConnectionT>::Invalidate() {
	valid = false;
}

template <typename ConnectionT>
void PooledConnection<ConnectionT>::ReturnToPool() noexcept {
	if (!pool || !connection) {
		return;
	}
	try {
		if (valid) {
			pool->Return(std::move(connection));
		} else {
			pool->Discard();
		}
	} catch (...) {
		try {
			pool->Discard();
		} catch (...) {
		}
	}
	pool = nullptr;
}

} // namespace pool
} // namespace dbconnector
