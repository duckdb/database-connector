#include "test_common.hpp"

#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"

#include "dbconnector/query/query_writer.hpp"
#include "dbconnector/table_scan/filter_pushdown.hpp"

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

static std::string TransformInFilter(const duckdb::LogicalType &type, duckdb::vector<duckdb::Value> values) {
	//! the Postgres blob literal configuration: '\x<hex>'::BYTEA
	auto config = dbconnector::table_scan::FilterPushdown::CreateConfig('"', '\'', QuoteEscapeStyle::DOUBLE_QUOTE,
	                                                                    "'\\x", "::BYTEA");
	auto column = duckdb::make_uniq<duckdb::BoundReferenceExpression>(type, 0);
	auto in_expr = duckdb::ExpressionFilter::CreateInExpression(std::move(column), std::move(values));
	duckdb::ExpressionFilter filter(std::move(in_expr));
	return dbconnector::table_scan::FilterPushdown::TransformFilter(config, "col", filter, 0);
}

TEST_CASE("Test filter pushdown IN list constants", group_name) {
	{
		//! blob constants have to be written with the constant configuration - with the identifier
		//! configuration the prefix and suffix are empty and EncodeBlob emits a bare 6470666B67'
		duckdb::vector<duckdb::Value> values;
		values.push_back(duckdb::Value::BLOB_RAW("dpfkg"));
		values.push_back(duckdb::Value::BLOB_RAW("other"));
		REQUIRE(TransformInFilter(duckdb::LogicalType::BLOB, std::move(values)) ==
		        "\"col\" IN ('\\x6470666B67'::BYTEA, '\\x6F74686572'::BYTEA)");
	}
	{
		duckdb::vector<duckdb::Value> values;
		//! quote and backslash bytes are hex encoded, not quote escaped
		values.push_back(duckdb::Value::BLOB_RAW("'\\"));
		REQUIRE(TransformInFilter(duckdb::LogicalType::BLOB, std::move(values)) == "\"col\" IN ('\\x275C'::BYTEA)");
	}
	{
		duckdb::vector<duckdb::Value> values;
		values.push_back(duckdb::Value::INTEGER(1));
		values.push_back(duckdb::Value::INTEGER(2));
		REQUIRE(TransformInFilter(duckdb::LogicalType::INTEGER, std::move(values)) == "\"col\" IN (1, 2)");
	}
	{
		duckdb::vector<duckdb::Value> values;
		values.push_back(duckdb::Value("foo"));
		REQUIRE(TransformInFilter(duckdb::LogicalType::VARCHAR, std::move(values)) == "\"col\" IN ('foo')");
	}
}
