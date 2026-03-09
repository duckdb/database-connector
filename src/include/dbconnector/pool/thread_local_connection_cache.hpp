#pragma once

#include <memory>

#include "dbconnector/pool/cached_connection.hpp"

namespace dbconnector {
namespace pool {

template <typename ConnectionT>
class ConnectionPool;

template <typename ConnectionT>
struct ThreadLocalConnectionCache {
	CachedConnection<ConnectionT> cached_conn;
	std::weak_ptr<ConnectionPool<ConnectionT>> owner;
	bool available = false;

	ThreadLocalConnectionCache() {
	}

	~ThreadLocalConnectionCache();

	void Clear();
};

} // namespace pool
} // namespace dbconnector
