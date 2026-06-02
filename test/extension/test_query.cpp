#include "test_common.hpp"

#include "dbconnector/query/query_writer.hpp"

static const std::string group_name = "[query]";

TEST_CASE("Test query writer identifier", group_name) {
	REQUIRE(dbconnector::query::QueryWriter::WriteIdentifier("foobar", '"') == "\"foobar\"");
	REQUIRE(dbconnector::query::QueryWriter::WriteIdentifier("foo\"bar", '"') == "\"foo\\\"bar\"");
}
