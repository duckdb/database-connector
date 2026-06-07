#include "dbconnector/table_scan/filter_pushdown.hpp"

#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/string_util.hpp"

#include "dbconnector/query/query_writer.hpp"

#include "dbconnector/table_scan/filter_util.hpp"
#include "dbconnector/table_scan/table_scan_exception.hpp"

namespace dbconnector {
namespace table_scan {

using namespace duckdb;

FilterPushdown::Config FilterPushdown::CreateConfig(char identifier_quote, query::QuoteEscapeStyle escape_style) {
	Config res;
	res.identifier_quote = identifier_quote;
	res.escape_style = escape_style;
	return res;
}

std::string FilterPushdown::CreateExpression(const std::string &column_name,
                                             const vector<unique_ptr<Expression>> &filters, const std::string &op) {
	vector<std::string> filter_entries;
	for (auto &filter : filters) {
		auto new_filter = TransformExpression(column_name, *filter);
		if (new_filter.empty()) {
			continue;
		}
		filter_entries.push_back(std::move(new_filter));
	}
	if (filter_entries.empty()) {
		return std::string();
	}
	return "(" + StringUtil::Join(filter_entries, " " + op + " ") + ")";
}

std::string FilterPushdown::TransformComparison(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		return "=";
	case ExpressionType::COMPARE_NOTEQUAL:
		return "!=";
	case ExpressionType::COMPARE_LESSTHAN:
		return "<";
	case ExpressionType::COMPARE_GREATERTHAN:
		return ">";
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return "<=";
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ">=";
	default:
		throw TableScanException("Unsupported expression type: '" + EnumUtil::ToString(type) + "'");
	}
}

static bool IsDirectReference(const Expression &expr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_REF:
	case ExpressionClass::BOUND_COLUMN_REF:
		return true;
	default:
		return false;
	}
}

std::string FilterPushdown::TransformExpression(const std::string &column_name, const Expression &expr) {
	if (BoundComparisonExpression::IsComparison(expr)) {
		auto &comparison = expr.Cast<BoundFunctionExpression>();
		auto comparison_type = comparison.GetExpressionType();
		auto &left = BoundComparisonExpression::Left(comparison);
		auto &right = BoundComparisonExpression::Right(comparison);
		const Value *constant = nullptr;
		if (IsDirectReference(left) && right.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			constant = &right.Cast<BoundConstantExpression>().GetValue();
		} else if (left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT && IsDirectReference(right)) {
			constant = &left.Cast<BoundConstantExpression>().GetValue();
			comparison_type = FlipComparisonExpression(comparison_type);
		} else {
			return std::string();
		}
		auto constant_string = query::QueryWriter::WriteConstant(*constant);
		auto operator_string = TransformComparison(comparison_type);
		return StringUtil::Format("%s %s %s", column_name, operator_string, constant_string);
	}

	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		switch (conjunction.GetExpressionType()) {
		case ExpressionType::CONJUNCTION_AND:
			return CreateExpression(column_name, conjunction.GetChildren(), "AND");
		case ExpressionType::CONJUNCTION_OR:
			return CreateExpression(column_name, conjunction.GetChildren(), "OR");
		default:
			return std::string();
		}
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op = expr.Cast<BoundOperatorExpression>();
		switch (op.GetExpressionType()) {
		case ExpressionType::OPERATOR_IS_NULL:
			if (op.GetChildren().size() == 1 && IsDirectReference(*op.GetChildren()[0])) {
				return column_name + " IS NULL";
			}
			return std::string();
		case ExpressionType::OPERATOR_IS_NOT_NULL:
			if (op.GetChildren().size() == 1 && IsDirectReference(*op.GetChildren()[0])) {
				return column_name + " IS NOT NULL";
			}
			return std::string();
		case ExpressionType::COMPARE_IN: {
			if (op.GetChildren().empty() || !IsDirectReference(*op.GetChildren()[0])) {
				return std::string();
			}
			std::string in_list;
			for (idx_t i = 1; i < op.GetChildren().size(); i++) {
				if (op.GetChildren()[i]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
					return std::string();
				}
				if (!in_list.empty()) {
					in_list += ", ";
				}
				in_list +=
				    query::QueryWriter::WriteConstant(op.GetChildren()[i]->Cast<BoundConstantExpression>().GetValue());
			}
			return column_name + " IN (" + in_list + ")";
		}
		default:
			return std::string();
		}
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (func.Function().GetName() == OptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<OptionalFilterFunctionData>();
			return data.child_filter_expr ? TransformExpression(column_name, *data.child_filter_expr) : std::string();
		}
		if (func.Function().GetName() == SelectivityOptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<SelectivityOptionalFilterFunctionData>();
			return data.child_filter_expr ? TransformExpression(column_name, *data.child_filter_expr) : std::string();
		}
		if (func.Function().GetName() == DynamicFilterScalarFun::NAME) {
			return std::string();
		}
		return std::string();
	}
	default:
		return std::string();
	}
}

std::string FilterPushdown::TransformFilter(const FilterPushdown::Config &config, const std::string &column_name,
                                            const TableFilter &filter) {
	auto query_config = query::QueryWriter::CreateConfig(config.identifier_quote, config.escape_style);
	std::string column_name_quoted = query::QueryWriter::WriteQuotedAndEscaped(query_config, column_name);
	auto &expr = FilterUtil::GetExpression(filter, "FilterPushdown::TransformFilter");
	return TransformExpression(column_name_quoted, expr);
}

} // namespace table_scan
} // namespace dbconnector
