#include "test_common.hpp"

#include "make_unique.hpp"
#include "pool.hpp"

static const std::string group_name = "[pool]";

class TestConnection {};

class TestConnectionPool : public dbconnector::pool::ConnectionPool<TestConnection> {
public:
	TestConnectionPool() : dbconnector::pool::ConnectionPool<TestConnection>(10, 600, true) {
	}

protected:
	std::unique_ptr<TestConnection> CreateNewConnection() override {
		return dbconnector::make_unique<TestConnection>();
	}

	bool CheckConnectionHealthy(TestConnection &conn) override {
		return true;
	}

	void ResetConnection(TestConnection &conn) override {
		// no-op
	}
};

TEST_CASE("Test connection pool basic", group_name) {
	auto pool = std::make_shared<TestConnectionPool>();
	REQUIRE(!!pool->Acquire());
	REQUIRE(!!pool->TryAcquire());
	REQUIRE(!!pool->ForceAcquire());
}
