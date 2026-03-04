#include "test_common.hpp"

#include "dbconnector/defer.hpp"

static const std::string group_name = "[defer]";

TEST_CASE("Test defer basic", group_name) {
	int a = 0;
	{
		auto deferred = dbconnector::Defer([&a]() { a += 1; });
		a += 1;
		REQUIRE(1 == a);
	}
	REQUIRE(2 == a);
}
