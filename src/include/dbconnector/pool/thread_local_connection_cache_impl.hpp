#pragma once

#include "thread_local_connection_cache.hpp"

#include <memory>

#include "connection_pool.hpp"

namespace dbconnector {
namespace pool {

template <typename ConnectionT>
ThreadLocalConnectionCache<ConnectionT>::~ThreadLocalConnectionCache() {
	auto pool = owner.lock();
	if (pool && connection) {
		pool->ReturnFromThreadLocalCache(std::move(connection));
	}
	connection.reset();
	owner.reset();
	available = false;
}

template <typename ConnectionT>
void ThreadLocalConnectionCache<ConnectionT>::Clear() {
	auto pool = owner.lock();
	if (connection && pool) {
		pool->ReturnFromThreadLocalCache(std::move(connection));
	}
	connection = nullptr;
	owner.reset();
	available = false;
}

} // namespace pool
} // namespace dbconnector
