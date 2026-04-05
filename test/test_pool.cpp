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
		return true;
	}

	void ResetConnection(TestConnection &) override {
		// no-op
	}

private:
	static dbconnector::pool::ConnectionPoolConfig CreateConfig(size_t max_connections, size_t timeout_ms,
	                                                            bool tl_cache_enabled = true) {
		dbconnector::pool::ConnectionPoolConfig config;
		config.max_connections = max_connections;
		config.wait_timeout_millis = timeout_ms;
		config.tl_cache_enabled = tl_cache_enabled;
		return config;
	}
};

TEST_CASE("Test connection pool basic", group_name) {
	auto pool = std::make_shared<TestConnectionPool>();
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
}

TEST_CASE("Test pool with a reaper", group_name) {
	auto pool = std::make_shared<TestConnectionPool>(4, 1000, false);
	REQUIRE(!pool->IsReaperRunning());
	REQUIRE(!pool->EnsureReaperRunning());
	pool->SetMaxLifetimeMillis(1000);
	REQUIRE(pool->EnsureReaperRunning());
	REQUIRE(pool->EnsureReaperRunning());
	REQUIRE(pool->IsReaperRunning());

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
}
