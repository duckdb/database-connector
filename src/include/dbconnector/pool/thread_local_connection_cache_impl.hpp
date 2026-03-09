#pragma once

#include "thread_local_connection_cache.hpp"

#include <memory>

#include "dbconnector/pool/connection_pool.hpp"

namespace dbconnector {
namespace pool {

template <typename ConnectionT>
ThreadLocalConnectionCache<ConnectionT>::~ThreadLocalConnectionCache() {
	auto pool = owner.lock();
	if (pool && cached_conn) {
		pool->ReturnFromThreadLocalCache(std::move(cached_conn));
	}
	cached_conn.reset();
	owner.reset();
	available = false;
}

template <typename ConnectionT>
void ThreadLocalConnectionCache<ConnectionT>::Clear() {
	auto pool = owner.lock();
	if (cached_conn && pool) {
		pool->ReturnFromThreadLocalCache(std::move(cached_conn));
	}
	cached_conn.reset();
	owner.reset();
	available = false;
}

} // namespace pool
} // namespace dbconnector
