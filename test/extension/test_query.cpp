#include "test_common.hpp"

#include "duckdb/common/types/time.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

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

static std::string TransformExpression(duckdb::unique_ptr<duckdb::Expression> expr) {
	//! the Postgres blob literal configuration: '\x<hex>'::BYTEA
	auto config = dbconnector::table_scan::FilterPushdown::CreateConfig('"', '\'', QuoteEscapeStyle::DOUBLE_QUOTE,
	                                                                    "'\\x", "'::BYTEA");
	duckdb::ExpressionFilter filter(std::move(expr));
	return dbconnector::table_scan::FilterPushdown::TransformFilter(config, "col", filter, 0);
}

static std::string TransformInFilter(const duckdb::LogicalType &type, duckdb::vector<duckdb::Value> values) {
	auto column = duckdb::make_uniq<duckdb::BoundReferenceExpression>(type, 0);
	return TransformExpression(duckdb::ExpressionFilter::CreateInExpression(std::move(column), std::move(values)));
}

TEST_CASE("Test filter pushdown IN list constants", group_name) {
	{
		//! blob constants have to be written with the constant configuration - with the identifier
		//! configuration the prefix and suffix are empty and EncodeBlob emits a bare 6470666B67'
		duckdb::vector<duckdb::Value> values;
		values.push_back(duckdb::Value::BLOB_RAW(std::string("dpfkg")));
		values.push_back(duckdb::Value::BLOB_RAW(std::string("other")));
		REQUIRE(TransformInFilter(duckdb::LogicalType::BLOB, std::move(values)) ==
		        "\"col\" IN ('\\x6470666B67'::BYTEA, '\\x6F74686572'::BYTEA)");
	}
	{
		duckdb::vector<duckdb::Value> values;
		//! quote and backslash bytes are hex encoded, not quote escaped
		values.push_back(duckdb::Value::BLOB_RAW(std::string("'\\")));
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

enum class ConjunctionChild { PUSHABLE, UNPUSHABLE, INTERNAL };

static std::string TransformConjunction(duckdb::ExpressionType conjunction_type, ConjunctionChild first_child) {
	auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER);
	duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> children;
	switch (first_child) {
	case ConjunctionChild::PUSHABLE:
		children.push_back(duckdb::ExpressionFilter::CreateNullCheckExpression(
		    duckdb::make_uniq<duckdb::BoundReferenceExpression>(type, 0),
		    duckdb::ExpressionType::OPERATOR_IS_NOT_NULL));
		break;
	case ConjunctionChild::UNPUSHABLE: {
		//! list constants are not pushed down
		auto constant = duckdb::Value::LIST(duckdb::LogicalType::INTEGER, {duckdb::Value::INTEGER(1)});
		children.push_back(duckdb::BoundComparisonExpression::Create(
		    duckdb::ExpressionType::COMPARE_EQUAL, duckdb::make_uniq<duckdb::BoundReferenceExpression>(type, 0),
		    duckdb::make_uniq<duckdb::BoundConstantExpression>(constant)));
		break;
	}
	case ConjunctionChild::INTERNAL:
		children.push_back(duckdb::CreateDynamicFilterExpression(nullptr, type));
		break;
	}
	children.push_back(duckdb::ExpressionFilter::CreateNullCheckExpression(
	    duckdb::make_uniq<duckdb::BoundReferenceExpression>(type, 0), duckdb::ExpressionType::OPERATOR_IS_NULL));
	return TransformExpression(duckdb::BoundConjunctionExpression::Create(conjunction_type, std::move(children)));
}

TEST_CASE("Test filter pushdown conjunctions", group_name) {
	auto conjunction_and = duckdb::ExpressionType::CONJUNCTION_AND;
	auto conjunction_or = duckdb::ExpressionType::CONJUNCTION_OR;
	REQUIRE(TransformConjunction(conjunction_and, ConjunctionChild::PUSHABLE) ==
	        "(\"col\" IS NOT NULL AND \"col\" IS NULL)");
	REQUIRE(TransformConjunction(conjunction_or, ConjunctionChild::PUSHABLE) ==
	        "(\"col\" IS NOT NULL OR \"col\" IS NULL)");
	REQUIRE(TransformConjunction(conjunction_and, ConjunctionChild::UNPUSHABLE).empty());
	REQUIRE(TransformConjunction(conjunction_or, ConjunctionChild::UNPUSHABLE).empty());
	REQUIRE(TransformConjunction(conjunction_and, ConjunctionChild::INTERNAL) == "(\"col\" IS NULL)");
	REQUIRE(TransformConjunction(conjunction_or, ConjunctionChild::INTERNAL).empty());
}

static std::string WriteTimestampTZ(duckdb::timestamp_t timestamp) {
	auto config = QueryWriter::CreateConfig('\'', QuoteEscapeStyle::DOUBLE_QUOTE);
	return QueryWriter::WriteConstant(config, duckdb::Value::TIMESTAMPTZ(duckdb::timestamp_tz_t(timestamp)));
}

TEST_CASE("Test query writer TIMESTAMPTZ constants", group_name) {
	auto date = duckdb::Date::FromDate(2024, 1, 1);
	REQUIRE(WriteTimestampTZ(duckdb::Timestamp::FromDatetime(date, duckdb::Time::FromTime(11, 30, 0, 500000))) ==
	        "'2024-01-01 11:30:00.5+00'");
	REQUIRE(WriteTimestampTZ(duckdb::timestamp_t::infinity()) == "'infinity'");
	REQUIRE(WriteTimestampTZ(duckdb::timestamp_t::ninfinity()) == "'-infinity'");
	auto config = QueryWriter::CreateConfig('\'', QuoteEscapeStyle::DOUBLE_QUOTE);
	REQUIRE(QueryWriter::WriteConstant(config, duckdb::Value(duckdb::LogicalType::TIMESTAMP_TZ)) == "NULL::VARCHAR");
}
