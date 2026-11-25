#include "test_common.hpp"

#include <memory>

#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"

#include "dbconnector/connect.hpp"

static const std::string group_name = "[connect]";

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

// explicit instantiation compiles all the members of the templates
template class dbconnector::connect::ConnectSessionState<TestConnection>;
template class dbconnector::connect::ConnectSessions<TestConnection>;

TEST_CASE("Test connect sessions impl can be compiled", group_name) {
	auto pool = std::make_shared<TestConnectionPool>();
	auto sessions = std::make_shared<dbconnector::connect::ConnectSessions<TestConnection>>(*pool, "test_connect");

	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto &context = *con.context;
	auto attached = db.instance->GetDatabaseManager().GetDatabase("memory");
	REQUIRE(attached);

	// the session of the client is reused
	auto conn_id = sessions->GetConnection(context, *attached);
	REQUIRE(sessions->GetConnection(context, *attached) == conn_id);

	// closing the session returns the connection to the pool, a new session is opened afterwards
	sessions->Close(context.GetConnectionId());
	sessions->Close(context.GetConnectionId());
	auto conn_id2 = sessions->GetConnection(context, *attached);
	REQUIRE(conn_id2 != 0);

	// destroying the sessions closes the open ones
	sessions.reset();
}
