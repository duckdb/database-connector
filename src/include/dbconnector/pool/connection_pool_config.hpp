#pragma once

#include <cstdint>
#include <algorithm>
#include <thread>

#include "dbconnector/pool/acquire_mode.hpp"

namespace dbconnector {
namespace pool {

struct ConnectionPoolConfig {
	AcquireMode acquire_mode = AcquireMode::FORCE;
	uint64_t max_connections = DefaultPoolSizeFromHardwareConcurrency(4, 32, 1.5);
	uint64_t wait_timeout_millis = 30000;
	bool tl_cache_enabled = false;
	uint64_t max_lifetime_millis = 0;
	uint64_t idle_timeout_millis = 60000;
	bool start_reaper_thread = true;

	static uint64_t DefaultPoolSizeFromHardwareConcurrency(uint64_t min_connections, uint64_t max_connections,
	                                                       float hardware_concurrency_coef) {
		uint64_t detected = static_cast<uint64_t>(std::thread::hardware_concurrency());
		uint64_t size = static_cast<uint64_t>(detected * hardware_concurrency_coef);
		size = (std::max<uint64_t>)(min_connections, size);
		size = (std::min<uint64_t>)(max_connections, size);
		return size;
	}
};

} // namespace pool
} // namespace dbconnector
