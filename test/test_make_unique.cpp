#include "test_common.hpp"

#include "make_unique.hpp"

static const std::string group_name = "[make_unique]";

TEST_CASE("Test make unique basic", group_name) {
	auto a = dbconnector::make_unique<int>();
	REQUIRE(!!a);
	*a = 42;
	REQUIRE(*a == 42);
}
