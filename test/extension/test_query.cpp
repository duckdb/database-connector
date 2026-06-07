#include "test_common.hpp"

#include "dbconnector/query/query_writer.hpp"

static const std::string group_name = "[query]";

using namespace dbconnector::query;

TEST_CASE("Test query writer identifier", group_name) {
	{
		auto config = QueryWriter::CreateConfig('"', QuoteEscapeStyle::BACKSLASH);
		REQUIRE(QueryWriter::WriteQuotedAndEscaped(config, "foobar") == "\"foobar\"");
		REQUIRE(QueryWriter::WriteQuotedAndEscaped(config, "foo\"bar\"baz") == "\"foo\\\"bar\\\"baz\"");
		REQUIRE(QueryWriter::WriteQuotedAndEscaped(config, "foo\\bar\\baz") == "\"foo\\\\bar\\\\baz\"");
	}
	{
		auto config = QueryWriter::CreateConfig('\'', QuoteEscapeStyle::DOUBLE_QUOTE);
		REQUIRE(QueryWriter::WriteQuotedAndEscaped(config, "foobar") == "'foobar'");
		REQUIRE(QueryWriter::WriteQuotedAndEscaped(config, "foo'bar'baz") == "'foo''bar''baz'");
		REQUIRE(QueryWriter::WriteQuotedAndEscaped(config, "foo\\bar\\baz") == "'foo\\bar\\baz'");
	}
}
