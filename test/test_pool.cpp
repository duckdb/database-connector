#include "test_common.hpp"

#include <chrono>
#include <cstdint>
#include <atomic>
#include <thread>

#include "dbconnector/defer.hpp"
#include "dbconnector/make_unique.hpp"
#include "dbconnector/pool.hpp"

static const std::string group_name = "[pool]";

static std::atomic<uint64_t> connection_id_counter(1);

namespace {

class TestConnection {
	uint64_t id;

public:
	TestConnection(uint64_t id_p) : id(id_p) {
	}

	uint64_t GetId() {
		return id;
	}
};

class TestConnectionPool : public dbconnector::pool::ConnectionPool<TestConnection> {
public:
	TestConnectionPool(size_t max_connections = 4, size_t timeout_ms = 30000, bool tl_cache_enabled = true)
	    : dbconnector::pool::ConnectionPool<TestConnection>(
	          CreateConfig(max_connections, timeout_ms, tl_cache_enabled)) {
	}

protected:
	std::unique_ptr<TestConnection> CreateNewConnection() override {
		return dbconnector::make_unique<TestConnection>(connection_id_counter.fetch_add(1, std::memory_order_relaxed));
	}

	bool CheckConnectionHealthy(TestConnection &) override {
		health_checks.fetch_add(1, std::memory_order_relaxed);
		return healthy.load(std::memory_order_relaxed);
	}

	void ResetConnection(TestConnection &) override {
		// no-op
	}

public:
	std::atomic<uint64_t> health_checks {0};
	std::atomic<bool> healthy {true};

private:
	static dbconnector::pool::ConnectionPoolConfig CreateConfig(size_t max_connections, size_t timeout_ms,
	                                                            bool tl_cache_enabled = true) {
		dbconnector::pool::ConnectionPoolConfig config;
		config.max_connections = max_connections;
		config.wait_timeout_millis = timeout_ms;
		config.tl_cache_enabled = tl_cache_enabled;
		config.idle_timeout_millis = 0;
		config.start_reaper_thread = false;
		config.health_check_query = "SELECT 42";
		return config;
	}
};

} // namespace

TEST_CASE("Test connection pool basic", group_name) {
	auto pool = std::make_shared<TestConnectionPool>();
	REQUIRE(pool->GetAcquireMode() == dbconnector::pool::AcquireMode::FORCE);
	REQUIRE(pool->WaitAcquire());
	REQUIRE(pool->TryAcquire());
	REQUIRE(pool->ForceAcquire());
	pool->SetMaxConnections(42);
	REQUIRE(pool->GetMaxConnections() == 42);
	pool->SetWaitTimeoutMillis(43);
	REQUIRE(pool->GetWaitTimeoutMillis() == 43);
	REQUIRE(pool->GetAvailableConnections() == 0);
	REQUIRE(pool->GetTotalConnections() == 1);
	REQUIRE(pool->IsThreadLocalCacheEnabled());
	pool->SetThreadLocalCacheEnabled(false);
	REQUIRE(!pool->IsThreadLocalCacheEnabled());
	pool->SetMaxLifetimeMillis(44);
	REQUIRE(pool->GetMaxLifetimeMillis() == 44);
	pool->SetIdleTimeoutMillis(45);
	REQUIRE(pool->GetIdleTimeoutMillis() == 45);

	REQUIRE(pool->GetCacheHits() == 2);
	REQUIRE(pool->GetCacheMisses() == 1);
	REQUIRE(pool->GetTryFailures() == 0);
}

TEST_CASE("Test connection pool acquire mode", group_name) {
	auto pool = std::make_shared<TestConnectionPool>(1);
	REQUIRE(pool->GetAcquireMode() == dbconnector::pool::AcquireMode::FORCE);
	REQUIRE(pool->GetMaxConnections() == 1);
	auto conn1 = pool->Acquire();
	REQUIRE(conn1);
	REQUIRE(pool->GetMaxConnections() == 1);
	{
		REQUIRE(pool->GetAvailableConnections() == 0);
		auto conn2 = pool->Acquire();
		REQUIRE(conn2);
		REQUIRE(pool->GetMaxConnections() == 1);
		REQUIRE(pool->GetAvailableConnections() == 0);
	}
	pool->SetAcquireMode("try");
	REQUIRE(!pool->Acquire());
	REQUIRE(pool->GetMaxConnections() == 1);
	REQUIRE(pool->GetAvailableConnections() == 0);

	REQUIRE(pool->GetCacheHits() == 0);
	REQUIRE(pool->GetCacheMisses() == 2);
	REQUIRE(pool->GetTryFailures() == 1);
}

TEST_CASE("Test pool size no thread-local", group_name) {
	auto pool = std::make_shared<TestConnectionPool>(2, 500, false);

	{
		auto conn_main = pool->WaitAcquire();
		REQUIRE(conn_main);
		REQUIRE(1 == pool->GetTotalConnections());
	}
	REQUIRE(1 == pool->GetTotalConnections());

	auto worker = [&pool]() {
		auto conn = pool->TryAcquire();
		REQUIRE_THREAD(conn);
		std::this_thread::sleep_for(std::chrono::milliseconds(1000));
	};
	std::thread th1(worker);
	std::thread th2(worker);
	auto deferred = dbconnector::Defer([&] {
		th1.join();
		th2.join();
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(300));
	REQUIRE(2 == pool->GetTotalConnections());

	bool timeout_thrown = false;
	try {
		auto conn_main = pool->WaitAcquire();
	} catch (const std::exception &e) {
		REQUIRE(0 == std::string(e.what()).find("Connection pool timeout"));
		timeout_thrown = true;
	}
	REQUIRE(timeout_thrown);

	REQUIRE(pool->GetCacheHits() == 1);
	REQUIRE(pool->GetCacheMisses() == 2);
	REQUIRE(pool->GetTryFailures() == 0);
}

TEST_CASE("Test pool size with thread-local", group_name) {
	auto pool = std::make_shared<TestConnectionPool>(2, 500, true);

	uint64_t conn_main_id = 0;
	{
		auto conn_main = pool->ForceAcquire();
		REQUIRE(conn_main);
		conn_main_id = conn_main.GetConnection().GetId();
		REQUIRE(conn_main_id > 0);
		REQUIRE(1 == pool->GetTotalConnections());
	}
	REQUIRE(1 == pool->GetTotalConnections());

	auto worker = [&pool]() {
		auto conn = pool->ForceAcquire();
		REQUIRE_THREAD(conn);
		std::this_thread::sleep_for(std::chrono::milliseconds(1000));
	};
	std::thread th1(worker);
	std::thread th2(worker);
	auto deferred = dbconnector::Defer([&] {
		th1.join();
		th2.join();
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(300));
	REQUIRE(3 == pool->GetTotalConnections());

	{
		auto conn_main = pool->WaitAcquire();
		REQUIRE(conn_main);
		REQUIRE(conn_main.GetConnection().GetId() == conn_main_id);
		REQUIRE(3 == pool->GetTotalConnections());
	}
	REQUIRE(2 == pool->GetTotalConnections());

	REQUIRE(pool->GetCacheHits() == 1);
	REQUIRE(pool->GetCacheMisses() == 3);
	REQUIRE(pool->GetTryFailures() == 0);
}

TEST_CASE("Test pool disabled", group_name) {
	auto pool = std::make_shared<TestConnectionPool>(0);

	REQUIRE_THROWS(pool->WaitAcquire());
	REQUIRE_THROWS(pool->TryAcquire());

	uint64_t conn1_id = 0;
	{
		auto conn1 = pool->ForceAcquire();
		REQUIRE(conn1);
		conn1_id = conn1.GetConnection().GetId();
		REQUIRE(conn1_id > 0);
		REQUIRE(pool->GetTotalConnections() == 0);
	}
	REQUIRE(pool->GetTotalConnections() == 0);
	{
		auto conn2 = pool->ForceAcquire();
		REQUIRE(conn2);
		REQUIRE(conn2.GetConnection().GetId() != conn1_id);
		REQUIRE(pool->GetTotalConnections() == 0);
	}
	REQUIRE(pool->GetTotalConnections() == 0);

	REQUIRE(pool->GetCacheHits() == 0);
	REQUIRE(pool->GetCacheMisses() == 2);
	REQUIRE(pool->GetTryFailures() == 0);
}

TEST_CASE("Test pool disable running", group_name) {
	auto pool = std::make_shared<TestConnectionPool>(4);

	auto conn1 = pool->TryAcquire();
	REQUIRE(conn1);

	uint64_t conn3_id = 0;
	{
		auto conn2 = pool->TryAcquire();
		REQUIRE(conn2);

		auto conn3 = pool->TryAcquire();
		conn3_id = conn3.GetConnection().GetId();
		REQUIRE(conn3_id > 0);

		REQUIRE(pool->GetTotalConnections() == 3);
	}
	REQUIRE(pool->GetTotalConnections() == 3);
	REQUIRE(pool->GetAvailableConnections() == 1);

	pool->SetMaxConnections(0);
	REQUIRE_THROWS(pool->WaitAcquire());
	REQUIRE_THROWS(pool->TryAcquire());

	REQUIRE(pool->GetTotalConnections() == 2);
	REQUIRE(pool->GetAvailableConnections() == 0);

	conn1.~PooledConnection();

	REQUIRE(pool->GetTotalConnections() == 1);
	REQUIRE(pool->GetAvailableConnections() == 0);

	{
		auto conn_tl = pool->ForceAcquire();
		REQUIRE(conn_tl);
		REQUIRE(conn_tl.GetConnection().GetId() == conn3_id);
	}

	REQUIRE(pool->GetTotalConnections() == 0);
	REQUIRE(pool->GetAvailableConnections() == 0);

	uint64_t conn4_id = 0;
	{
		auto conn4 = pool->ForceAcquire();
		REQUIRE(conn4);
		conn4_id = conn4.GetConnection().GetId();
		REQUIRE(conn4_id > conn3_id);
		REQUIRE(pool->GetTotalConnections() == 0);
	}
	REQUIRE(pool->GetTotalConnections() == 0);

	{
		auto conn5 = pool->ForceAcquire();
		REQUIRE(conn5);
		REQUIRE(conn5.GetConnection().GetId() > conn4_id);
		REQUIRE(pool->GetTotalConnections() == 0);
	}
	REQUIRE(pool->GetTotalConnections() == 0);

	REQUIRE(pool->GetCacheHits() == 1);
	REQUIRE(pool->GetCacheMisses() == 5);
	REQUIRE(pool->GetTryFailures() == 0);
}

TEST_CASE("Test pool with a reaper", group_name) {
	auto pool = std::make_shared<TestConnectionPool>(4, 1000, false);
	REQUIRE(!pool->IsReaperRunning());
	REQUIRE(!pool->EnsureReaperRunning());
	pool->SetMaxLifetimeMillis(1000);
	REQUIRE(pool->EnsureReaperRunning());
	REQUIRE(pool->EnsureReaperRunning());
	REQUIRE(pool->IsReaperRunning());
	REQUIRE(pool->GetReaperPeriodMillis() == 1000);

	{
		auto conn = pool->WaitAcquire();
		REQUIRE(conn);
		REQUIRE(1 == pool->GetTotalConnections());
	}
	REQUIRE(1 == pool->GetTotalConnections());

	std::this_thread::sleep_for(std::chrono::milliseconds(500));
	REQUIRE(1 == pool->GetTotalConnections());

	std::this_thread::sleep_for(std::chrono::milliseconds(1500));
	REQUIRE(0 == pool->GetTotalConnections());

	pool->SetIdleTimeoutMillis(1000);
	pool->SetMaxLifetimeMillis(0);

	{
		auto conn = pool->WaitAcquire();
		REQUIRE(conn);
		REQUIRE(1 == pool->GetTotalConnections());
	}
	REQUIRE(1 == pool->GetTotalConnections());

	std::this_thread::sleep_for(std::chrono::milliseconds(500));
	REQUIRE(1 == pool->GetTotalConnections());

	std::this_thread::sleep_for(std::chrono::milliseconds(1500));
	REQUIRE(0 == pool->GetTotalConnections());

	REQUIRE(pool->GetCacheHits() == 0);
	REQUIRE(pool->GetCacheMisses() == 2);
	REQUIRE(pool->GetTryFailures() == 0);
}

TEST_CASE("Test pool with a reaper restart", group_name) {
	auto pool = std::make_shared<TestConnectionPool>(4, 1000, false);
	REQUIRE(!pool->IsReaperRunning());
	REQUIRE(!pool->EnsureReaperRunning());
	pool->SetMaxLifetimeMillis(1000);
	REQUIRE(pool->EnsureReaperRunning());
	REQUIRE(pool->IsReaperRunning());
	pool->ShutdownReaper();
	REQUIRE(!pool->IsReaperRunning());

	{
		auto conn = pool->WaitAcquire();
		REQUIRE(conn);
		REQUIRE(1 == pool->GetTotalConnections());
	}
	REQUIRE(1 == pool->GetTotalConnections());

	std::this_thread::sleep_for(std::chrono::milliseconds(500));
	REQUIRE(1 == pool->GetTotalConnections());

	std::this_thread::sleep_for(std::chrono::milliseconds(1500));
	REQUIRE(1 == pool->GetTotalConnections());

	REQUIRE(pool->EnsureReaperRunning());

	std::this_thread::sleep_for(std::chrono::milliseconds(500));
	REQUIRE(1 == pool->GetTotalConnections());

	std::this_thread::sleep_for(std::chrono::milliseconds(1500));
	REQUIRE(0 == pool->GetTotalConnections());

	REQUIRE(pool->GetCacheHits() == 0);
	REQUIRE(pool->GetCacheMisses() == 1);
	REQUIRE(pool->GetTryFailures() == 0);
}

TEST_CASE("Test connection pool connection ids", group_name) {
	auto pool = std::make_shared<TestConnectionPool>(3);
	auto conn1 = pool->WaitAcquire();
	auto conn2 = pool->TryAcquire();
	auto conn3 = pool->ForceAcquire();
	auto conn4 = pool->TryAcquire();
	REQUIRE(conn1.Id() > 0);
	REQUIRE(conn2.Id() > 0);
	REQUIRE(conn3.Id() > 0);
	REQUIRE(conn4.Id() == 0);
	REQUIRE(conn1.Id() != conn2.Id());
	REQUIRE(conn2.Id() != conn3.Id());
	conn4 = std::move(conn3);
	REQUIRE(conn4.Id() > 0);
	REQUIRE(conn3.Id() == 0);
	uint64_t conn4_id = conn4.Id();
	conn2 = std::move(conn4);
	REQUIRE(conn2.Id() == conn4_id);
	REQUIRE(conn4.Id() == 0);
}

TEST_CASE("Test connection pool pinning", group_name) {
	auto pool = std::make_shared<TestConnectionPool>(1);
	auto conn1 = pool->TryAcquire();
	REQUIRE(conn1);
	uint64_t conn1_id = conn1.Id();
	REQUIRE(pool->GetAvailableConnections() == 0);
	REQUIRE(pool->GetTotalConnections() == 1);
	REQUIRE(pool->GetPinnedConnections() == 0);
	pool->PinConnection(std::move(conn1));
	REQUIRE(pool->GetAvailableConnections() == 0);
	REQUIRE(pool->GetTotalConnections() == 1);
	REQUIRE(pool->GetPinnedConnections() == 1);
	auto unpinned = pool->UnpinConnection(conn1_id);
	REQUIRE(pool->GetAvailableConnections() == 0);
	REQUIRE(pool->GetTotalConnections() == 1);
	REQUIRE(pool->GetPinnedConnections() == 0);
	REQUIRE(unpinned.Id() == conn1_id);
}

TEST_CASE("Test connection pool pinning with pool disabled", group_name) {
	auto pool = std::make_shared<TestConnectionPool>(0);
	auto conn1 = pool->ForceAcquire();
	REQUIRE(conn1);
	uint64_t conn1_id = conn1.Id();
	uint64_t conn1_conn_id = conn1.GetConnection().GetId();
	REQUIRE(conn1.OriginatesFrom(pool.get()));
	REQUIRE(pool->PinConnection(std::move(conn1)) == conn1_id);
	REQUIRE(pool->GetPinnedConnections() == 1);
	REQUIRE(pool->GetTotalConnections() == 0);
	{
		// the connection can be taken out and pinned back multiple times
		auto unpinned = pool->UnpinConnection(conn1_id);
		REQUIRE(unpinned.GetConnection().GetId() == conn1_conn_id);
		REQUIRE(pool->GetPinnedConnections() == 0);
		REQUIRE(pool->PinConnection(std::move(unpinned)) == conn1_id);
		REQUIRE(pool->GetPinnedConnections() == 1);
	}
	{
		// the connection can be pinned back without a reference to the pool
		auto unpinned = pool->UnpinConnection(conn1_id);
		REQUIRE(pool->GetPinnedConnections() == 0);
		unpinned.PinBack();
		REQUIRE_FALSE(unpinned);
		REQUIRE(pool->GetPinnedConnections() == 1);
		REQUIRE(pool->GetTotalConnections() == 0);
	}
	{
		// once unpinned and released the connection is closed - not returned to the pool
		auto unpinned = pool->UnpinConnection(conn1_id);
	}
	REQUIRE(pool->GetPinnedConnections() == 0);
	REQUIRE(pool->GetAvailableConnections() == 0);
	REQUIRE(pool->GetTotalConnections() == 0);

	// an unpooled connection cannot be pinned in another pool
	auto other_pool = std::make_shared<TestConnectionPool>(0);
	auto conn2 = pool->ForceAcquire();
	REQUIRE_FALSE(conn2.OriginatesFrom(other_pool.get()));
	REQUIRE_THROWS(other_pool->PinConnection(std::move(conn2)));
}

TEST_CASE("Test connection pool health check query", group_name) {
	auto pool = std::make_shared<TestConnectionPool>(1);
	REQUIRE(pool->GetHealthCheckQuery() == "SELECT 42");
	pool->SetHealthCheckQuery("SELECT 43");
	REQUIRE(pool->GetHealthCheckQuery() == "SELECT 43");
}

TEST_CASE("Test connection pool alive bypass window", group_name) {
	for (bool tl_cache_enabled : {false, true}) {
		auto pool = std::make_shared<TestConnectionPool>(4, 1000, tl_cache_enabled);
		REQUIRE(pool->GetAliveBypassWindowMillis() == 500);

		// new connection is not checked
		{ auto conn = pool->WaitAcquire(); }
		REQUIRE(pool->health_checks == 0);

		// recently returned connection is not checked
		{ auto conn = pool->WaitAcquire(); }
		REQUIRE(pool->health_checks == 0);

		// connection idle for longer than the window is checked
		std::this_thread::sleep_for(std::chrono::milliseconds(600));
		{ auto conn = pool->WaitAcquire(); }
		REQUIRE(pool->health_checks == 1);

		// bypass disabled, every reuse is checked
		pool->SetAliveBypassWindowMillis(0);
		REQUIRE(pool->GetAliveBypassWindowMillis() == 0);
		{ auto conn = pool->WaitAcquire(); }
		{ auto conn = pool->TryAcquire(); }
		{ auto conn = pool->ForceAcquire(); }
		REQUIRE(pool->health_checks == 4);

		REQUIRE(pool->GetTotalConnections() == 1);
		REQUIRE(pool->GetCacheMisses() == 1);
		if (tl_cache_enabled) {
			REQUIRE(pool->GetThreadLocalCacheHits() == 5);
		} else {
			REQUIRE(pool->GetThreadLocalCacheHits() == 0);
		}
	}
}

TEST_CASE("Test connection pool unhealthy connection", group_name) {
	for (bool tl_cache_enabled : {false, true}) {
		auto pool = std::make_shared<TestConnectionPool>(4, 1000, tl_cache_enabled);
		pool->SetAliveBypassWindowMillis(0);

		uint64_t first_conn_id = 0;
		{
			auto conn = pool->WaitAcquire();
			first_conn_id = conn.GetConnection().GetId();
		}

		// unhealthy connection is discarded on acquire and replaced with a new one
		pool->healthy = false;
		uint64_t second_conn_id = 0;
		{
			auto conn = pool->WaitAcquire();
			second_conn_id = conn.GetConnection().GetId();
			REQUIRE(second_conn_id != first_conn_id);
			REQUIRE(pool->GetTotalConnections() == 1);
			// return time is only tracked when bypass (or timeouts) are enabled
			pool->SetAliveBypassWindowMillis(500);
		}
		// the unhealthy connection is checked once, also when it is taken from the thread-local cache
		REQUIRE(pool->health_checks == 1);
		REQUIRE(pool->GetTotalConnections() == 1);

		// within the bypass window the connection is reused without a check
		uint64_t checks_before = pool->health_checks;
		{
			auto conn = pool->WaitAcquire();
			REQUIRE(conn.GetConnection().GetId() == second_conn_id);
		}
		REQUIRE(pool->health_checks == checks_before);
	}
}

namespace {

class TestNoRoundTripCheckPool : public TestConnectionPool {
public:
	TestNoRoundTripCheckPool(bool tl_cache_enabled) : TestConnectionPool(4, 1000, tl_cache_enabled) {
	}

protected:
	bool CheckConnectionAliveNoRoundTrip(TestConnection &) override {
		no_round_trip_checks.fetch_add(1, std::memory_order_relaxed);
		return alive.load(std::memory_order_relaxed);
	}

public:
	std::atomic<uint64_t> no_round_trip_checks {0};
	std::atomic<bool> alive {true};
};

} // namespace

TEST_CASE("Test connection pool no round trip check in alive bypass window", group_name) {
	for (bool tl_cache_enabled : {false, true}) {
		auto pool = std::make_shared<TestNoRoundTripCheckPool>(tl_cache_enabled);
		REQUIRE(pool->GetAliveBypassWindowMillis() == 500);

		// new connection is not checked
		uint64_t first_conn_id = 0;
		{
			auto conn = pool->WaitAcquire();
			first_conn_id = conn.GetConnection().GetId();
		}
		REQUIRE(pool->no_round_trip_checks == 0);
		REQUIRE(pool->health_checks == 0);

		// within the window only the no round trip check is called, with every acquire mode
		{ auto conn = pool->WaitAcquire(); }
		{ auto conn = pool->TryAcquire(); }
		{ auto conn = pool->ForceAcquire(); }
		REQUIRE(pool->no_round_trip_checks == 3);
		REQUIRE(pool->health_checks == 0);

		// connection that fails the no round trip check is discarded and replaced with a new one
		pool->alive = false;
		uint64_t second_conn_id = 0;
		{
			auto conn = pool->WaitAcquire();
			second_conn_id = conn.GetConnection().GetId();
			REQUIRE(second_conn_id != first_conn_id);
			REQUIRE(pool->GetTotalConnections() == 1);
		}
		// the connection that fails the check is discarded right away, also the thread-local one,
		// it is not checked once more in the shared pool
		uint64_t checks_after_discard = 4;
		REQUIRE(pool->no_round_trip_checks == checks_after_discard);
		REQUIRE(pool->health_checks == 0);
		REQUIRE(pool->GetCacheMisses() == 2);

		// outside of the window only the full health check is called
		pool->alive = true;
		std::this_thread::sleep_for(std::chrono::milliseconds(600));
		{
			auto conn = pool->WaitAcquire();
			REQUIRE(conn.GetConnection().GetId() == second_conn_id);
		}
		REQUIRE(pool->no_round_trip_checks == checks_after_discard);
		REQUIRE(pool->health_checks == 1);

		// bypass disabled, the no round trip check is never called
		pool->SetAliveBypassWindowMillis(0);
		{ auto conn = pool->WaitAcquire(); }
		REQUIRE(pool->no_round_trip_checks == checks_after_discard);
		REQUIRE(pool->health_checks == 2);
		REQUIRE(pool->GetTotalConnections() == 1);
	}
}
