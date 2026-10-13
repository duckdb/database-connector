#include "dbconnector/table_scan/filter_util.hpp"

#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

namespace dbconnector {
namespace table_scan {

const duckdb::Expression &FilterUtil::GetExpression(const duckdb::TableFilter &filter,
                                                    const std::string &call_context) {
	auto &expr_filter = duckdb::ExpressionFilter::GetExpressionFilter(filter, call_context.c_str());
	return *expr_filter.expr;
}

bool FilterUtil::IsInternalFilter(const duckdb::TableFilter &filter) {
	return IsInternalExpression(GetExpression(filter, "FilterPushdown::IsInternalFilter"));
}

bool FilterUtil::IsInternalExpression(const duckdb::Expression &expr) {
	return expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_FUNCTION &&
	       duckdb::TableFilterFunctions::IsTableFilterFunction(expr.Cast<duckdb::BoundFunctionExpression>().Function());
}

std::string FilterUtil::ToString(const duckdb::TableFilter &filter) {
	auto &expr = GetExpression(filter, "FilterPushdown::ToString");
	return expr.ToString();
}

} // namespace table_scan
} // namespace dbconnector
