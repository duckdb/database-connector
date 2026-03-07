#include "test_common.hpp"

#include <chrono>
#include <thread>

#include "dbconnector/defer.hpp"
#include "dbconnector/make_unique.hpp"
#include "dbconnector/pool.hpp"

static const std::string group_name = "[pool]";

class TestConnection {};

class TestConnectionPool : public dbconnector::pool::ConnectionPool<TestConnection> {
public:
	TestConnectionPool(size_t max_connections = DEFAULT_POOL_SIZE, size_t timeout_ms = DEFAULT_POOL_TIMEOUT_MS,
	                   bool thread_local_cache_enabled = true)
	    : dbconnector::pool::ConnectionPool<TestConnection>(max_connections, timeout_ms, thread_local_cache_enabled) {
	}

protected:
	std::unique_ptr<TestConnection> CreateNewConnection() override {
		return dbconnector::make_unique<TestConnection>();
	}

	bool CheckConnectionHealthy(TestConnection &) override {
		return true;
	}

	void ResetConnection(TestConnection &) override {
		// no-op
	}
};

TEST_CASE("Test connection pool basic", group_name) {
	auto pool = std::make_shared<TestConnectionPool>();
	REQUIRE(!!pool->Acquire());
	REQUIRE(!!pool->TryAcquire());
	REQUIRE(!!pool->ForceAcquire());
}

TEST_CASE("Test pool size no thread-local", group_name) {
	auto pool = std::make_shared<TestConnectionPool>(2, 500, false);

	{
		auto conn_main = pool->Acquire();
		REQUIRE(conn_main);
		REQUIRE(1 == pool->GetTotalConnections());
	}
	REQUIRE(1 == pool->GetTotalConnections());

	auto worker = [&pool]() {
		auto conn = pool->TryAcquire();
		REQUIRE(conn);
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
		auto conn_main = pool->Acquire();
	} catch (const std::exception &e) {
		REQUIRE(0 == std::string(e.what()).find("Connection pool timeout"));
		timeout_thrown = true;
	}
	REQUIRE(timeout_thrown);
}

TEST_CASE("Test pool size with thread-local", group_name) {
	auto pool = std::make_shared<TestConnectionPool>(2, 500, true);

	{
		auto conn_main = pool->ForceAcquire();
		REQUIRE(conn_main);
		REQUIRE(1 == pool->GetTotalConnections());
	}
	REQUIRE(1 == pool->GetTotalConnections());

	auto worker = [&pool]() {
		auto conn = pool->ForceAcquire();
		REQUIRE(conn);
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
		auto conn_main = pool->Acquire();
		REQUIRE(conn_main);
		REQUIRE(3 == pool->GetTotalConnections());
	}
	REQUIRE(2 == pool->GetTotalConnections());
}
