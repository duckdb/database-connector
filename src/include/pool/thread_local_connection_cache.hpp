#pragma once

#include <memory>

namespace dbconnector {
namespace pool {

template <typename ConnectionT>
class ConnectionPool;

template <typename ConnectionT>
struct ThreadLocalConnectionCache {
	std::unique_ptr<ConnectionT> connection;
	std::weak_ptr<ConnectionPool<ConnectionT>> owner;
	bool available = false;

	ThreadLocalConnectionCache() {
	}

	~ThreadLocalConnectionCache();

	void Clear();
};

} // namespace pool
} // namespace dbconnector
