#pragma once

#include "pooled_connection.hpp"

#include "dbconnector/pool/connection_pool.hpp"
#include "dbconnector/pool/pool_exception.hpp"

namespace dbconnector {
namespace pool {

template <typename ConnectionT>
PooledConnection<ConnectionT>::PooledConnection()
    : pool(nullptr), connection(nullptr), valid(false), created_at(std::chrono::steady_clock::time_point()) {
}

template <typename ConnectionT>
PooledConnection<ConnectionT>::PooledConnection(std::shared_ptr<ConnectionPool<ConnectionT>> pool_p,
                                                std::unique_ptr<ConnectionT> connection_p,
                                                std::chrono::steady_clock::time_point created_at_p)
    : pool(std::move(pool_p)), connection(std::move(connection_p)), valid(true), created_at(created_at_p) {
}

template <typename ConnectionT>
PooledConnection<ConnectionT>::PooledConnection(std::shared_ptr<ConnectionPool<ConnectionT>> pool_p,
                                                CachedConnection<ConnectionT> cached_conn_p)
    : PooledConnection<ConnectionT>(std::move(pool_p), cached_conn_p.TakeConnection(), cached_conn_p.GetCreatedAt()) {
}

template <typename ConnectionT>
PooledConnection<ConnectionT>::~PooledConnection() noexcept {
	ReturnToPool();
}

template <typename ConnectionT>
PooledConnection<ConnectionT>::PooledConnection(PooledConnection &&other) noexcept
    : pool(std::move(other.pool)), connection(std::move(other.connection)), valid(other.valid),
      created_at(other.created_at) {
	other.valid = false;
}

template <typename ConnectionT>
PooledConnection<ConnectionT> &PooledConnection<ConnectionT>::operator=(PooledConnection &&other) noexcept {
	if (this != &other) {
		ReturnToPool();
		this->pool = std::move(other.pool);
		this->connection = std::move(other.connection);
		this->valid = other.valid;
		this->created_at = other.created_at;
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
	return connection.get() != nullptr && valid;
}

template <typename ConnectionT>
void PooledConnection<ConnectionT>::Invalidate() {
	valid = false;
}

template <typename ConnectionT>
std::chrono::steady_clock::time_point PooledConnection<ConnectionT>::GetCreatedAt() {
	return created_at;
}

template <typename ConnectionT>
void PooledConnection<ConnectionT>::ReturnToPool() noexcept {
	if (!pool || !connection) {
		return;
	}
	try {
		if (valid) {
			pool->Return(std::move(connection), created_at);
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
