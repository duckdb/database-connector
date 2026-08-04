#include "test_common.hpp"

#include <memory>

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/table_function.hpp"

#include "dbconnector/functions/configure_pool.hpp"

static const std::string group_name = "[configure_pool]";

namespace {

class TestConnection {};

class TestConnectionPool : public dbconnector::pool::ConnectionPool<TestConnection> {
public:
	TestConnectionPool()
	    : dbconnector::pool::ConnectionPool<TestConnection>(dbconnector::pool::ConnectionPoolConfig()) {
	}

protected:
	std::unique_ptr<TestConnection> CreateNewConnection() override {
		return std::make_unique<TestConnection>();
	}

	bool CheckConnectionHealthy(TestConnection &) override {
		return true;
	}

	void ResetConnection(TestConnection &) override {
		// no-op
	}
};

} // namespace

static duckdb::shared_ptr<TestConnectionPool> GetConnnectionPoolFromCatalog(duckdb::Catalog &) {
	return nullptr;
}

class TestConfigurePoolFunction : public duckdb::TableFunction {
public:
	TestConfigurePoolFunction()
	    : TableFunction("test_configure_pool", std::vector<duckdb::LogicalType>(),
	                    dbconnector::functions::ConfigurePool::Function<TestConnection, GetConnnectionPoolFromCatalog>,
	                    dbconnector::functions::ConfigurePool::Bind,
	                    dbconnector::functions::ConfigurePool::InitGlobalState,
	                    dbconnector::functions::ConfigurePool::InitLocalState) {
		for (auto &en : dbconnector::functions::ConfigurePool::NamedParameters()) {
			named_parameters[en.first] = en.second;
		}
	}
};

TEST_CASE("Test configure pool impl cam be compiled", group_name) {
	TestConfigurePoolFunction func;
	(void)func;
}
