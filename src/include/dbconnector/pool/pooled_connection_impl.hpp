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
    : id(NextId()), pool(std::move(pool_p)), connection(std::move(connection_p)), valid(true),
      created_at(created_at_p) {
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
    : id(other.id), pool(std::move(other.pool)), connection(std::move(other.connection)), valid(other.valid),
      created_at(other.created_at) {
	other.id = 0;
	other.valid = false;
}

template <typename ConnectionT>
PooledConnection<ConnectionT> &PooledConnection<ConnectionT>::operator=(PooledConnection &&other) noexcept {
	if (this != &other) {
		ReturnToPool();
		this->id = other.id;
		other.id = 0;
		this->pool = std::move(other.pool);
		this->connection = std::move(other.connection);
		this->valid = other.valid;
		other.valid = false;
		this->created_at = other.created_at;
	}
	return *this;
}

template <typename ConnectionT>
uint64_t PooledConnection<ConnectionT>::Id() {
	return id;
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
bool PooledConnection<ConnectionT>::OriginatesFrom(ConnectionPool<ConnectionT> *conn_pool) {
	return pool.get() == conn_pool;
}

template <typename ConnectionT>
void PooledConnection<ConnectionT>::PinBack() {
	if (!pool) {
		throw PoolException("Cannot pin the connection, ID: " + std::to_string(Id()) +
		                    " back because it is does not belong to the pool");
	}
	pool->PinConnection(std::move(*this));
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

template <typename ConnectionT>
uint64_t PooledConnection<ConnectionT>::NextId() {
	static std::atomic<uint64_t> id_counter {0};
	uint64_t next = id_counter.fetch_add(1, std::memory_order_acq_rel);
	if (next != 0) {
		return next;
	}
	return id_counter.fetch_add(1, std::memory_order_acq_rel);
}

} // namespace pool
} // namespace dbconnector
