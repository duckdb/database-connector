#pragma once

#include <cstdint>

namespace dbconnector {
namespace pool {

struct ConnectionPoolConfig {
	uint64_t max_connections = 4;
	uint64_t wait_timeout_millis = 30000;
	bool tl_cache_enabled = true;
	uint64_t max_lifetime_millis = 0;
	uint64_t idle_timeout_millis = 0;
	bool start_reaper_thread = false;
};

} // namespace pool
} // namespace dbconnector
