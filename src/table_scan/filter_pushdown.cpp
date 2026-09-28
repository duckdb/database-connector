#include "dbconnector/table_scan/filter_pushdown.hpp"

#include "duckdb/function/scalar/struct_utils.hpp"
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

FilterPushdown::Config
FilterPushdown::CreateConfig(char identifier_quote, char constant_quote, query::QuoteEscapeStyle escape_style,
                             const string &blob_literal_prefix, const string &blob_literal_suffix,
                             const std::string &varchar_comparison_collation, write_distinct_from_t write_distinct_from,
                             get_constant_range_t get_constant_range) {
	Config res;
	res.identifier_config = query::QueryWriter::CreateConfig(identifier_quote, escape_style);
	res.constant_config =
	    query::QueryWriter::CreateConfig(constant_quote, escape_style, blob_literal_prefix, blob_literal_suffix);
	res.varchar_comparison_collation = varchar_comparison_collation;
	res.write_distinct_from = write_distinct_from;
	res.get_constant_range = get_constant_range;
	return res;
}

static string GetComparizonOperator(ExpressionType type) {
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
		// unsupported comparison type
		return string();
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

static string WriteIsNull(const string &column_name, const BoundOperatorExpression &op) {
	if (op.GetChildren().size() == 1 && IsDirectReference(*op.GetChildren()[0])) {
		return column_name + " IS NULL";
	}
	return string();
}

static string WriteIsNotNull(const string &column_name, const BoundOperatorExpression &op) {
	if (op.GetChildren().size() == 1 && IsDirectReference(*op.GetChildren()[0])) {
		return column_name + " IS NOT NULL";
	}
	return string();
}

//! Serialize a comparison against a constant that compares above / below every value the column
//! can contain - the comparison is always true (for non-NULL values) or always false
static string WriteNonFiniteComparison(const string &column_name, ExpressionType comparison_type,
                                       dbconnector::table_scan::FilterConstantRange range) {
	using dbconnector::table_scan::FilterConstantRange;

	bool always_true;
	switch (comparison_type) {
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		always_true = range == FilterConstantRange::ABOVE_ALL_VALUES;
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		always_true = range == FilterConstantRange::BELOW_ALL_VALUES;
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		always_true = true;
		break;
	case ExpressionType::COMPARE_EQUAL:
		always_true = false;
		break;
	default:
		return string();
	}
	// note: a NULL value compares as NULL and is filtered out either way, matching IS NOT NULL
	return always_true ? column_name + " IS NOT NULL" : "FALSE";
}

static string WriteComparison(const FilterPushdown::Config &config, const string &column_name,
                              ExpressionType comparison_type, const Value &constant) {
	if (config.get_constant_range) {
		auto range = config.get_constant_range(constant);
		if (range != FilterConstantRange::FINITE) {
			return WriteNonFiniteComparison(column_name, comparison_type, range);
		}
	}
	string constant_string = query::QueryWriter::WriteConstant(config.constant_config, constant);
	if ((comparison_type == ExpressionType::COMPARE_DISTINCT_FROM ||
	     comparison_type == ExpressionType::COMPARE_NOT_DISTINCT_FROM) &&
	    config.write_distinct_from) {
		return config.write_distinct_from(comparison_type, column_name, constant_string);
	}
	string operator_string = GetComparizonOperator(comparison_type);
	if (operator_string.empty()) {
		return string();
	}
	return StringUtil::Format("%s %s %s", column_name, operator_string, constant_string);
}

static string WriteConjunction(const FilterPushdown::Config &config, const string &column_name,
                               const vector<unique_ptr<Expression>> &filters, const string &op) {
	vector<string> filter_entries;
	for (auto &filter : filters) {
		auto new_filter = FilterPushdown::TransformFilterExpression(config, column_name, *filter);
		if (new_filter.empty()) {
			continue;
		}
		filter_entries.push_back(std::move(new_filter));
	}
	if (filter_entries.empty()) {
		return string();
	}
	return "(" + StringUtil::Join(filter_entries, " " + op + " ") + ")";
}

static string WriteCompareIn(const FilterPushdown::Config &config, const string &column_name,
                             const BoundOperatorExpression &op) {
	if (op.GetChildren().empty() || !IsDirectReference(*op.GetChildren()[0])) {
		return string();
	}
	string in_list;
	for (idx_t i = 1; i < op.GetChildren().size(); i++) {
		if (op.GetChildren()[i]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
			return string();
		}
		auto &constant = op.GetChildren()[i]->Cast<BoundConstantExpression>().GetValue();
		if (config.get_constant_range && config.get_constant_range(constant) != FilterConstantRange::FINITE) {
			// the column can never contain a non-finite value - drop the element
			continue;
		}
		if (!in_list.empty()) {
			in_list += ", ";
		}
		in_list += query::QueryWriter::WriteConstant(config.constant_config, constant);
	}
	if (in_list.empty()) {
		// all elements were non-finite - the filter matches no rows
		return "FALSE";
	}
	return column_name + " IN (" + in_list + ")";
}

static string WriteConstantFilter(const FilterPushdown::Config &config, const string &column_name,
                                  ExpressionType comparison_type, const Value &constant) {
	string comparison = WriteComparison(config, column_name, comparison_type, constant);
	if (constant.type().id() == LogicalTypeId::VARCHAR && !config.varchar_comparison_collation.empty()) {
		string collation =
		    query::QueryWriter::WriteQuotedAndEscaped(config.identifier_config, config.varchar_comparison_collation);
		comparison += " COLLATE " + collation;
	}
	return comparison;
}

static string TransformComparison(const FilterPushdown::Config &config, const string &column_name,
                                  const Expression &expr) {
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
		return string();
	}
	if (config.get_constant_range) {
		auto constant_range = config.get_constant_range(*constant);
		if (constant_range != FilterConstantRange::FINITE) {
			// the constant cannot be represented in MySQL - but the column can never contain
			// a non-finite value either, so the comparison has a known outcome
			return WriteNonFiniteComparison(column_name, comparison_type, constant_range);
		}
	}
	auto constant_string = query::QueryWriter::WriteConstant(config.constant_config, *constant);
	return WriteComparison(config, column_name, comparison_type, constant_string);
}

static string TransformExpressionSubject(const FilterPushdown::Config &config, const string &column_name,
                                         const Expression &expr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_REF:
	case ExpressionClass::BOUND_COLUMN_REF:
		return column_name;
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func = expr.Cast<BoundFunctionExpression>();
		idx_t child_idx;
		if (!TryGetStructExtractChildIndex(func, child_idx) || func.GetChildren().empty()) {
			return string();
		}
		auto parent_name = TransformExpressionSubject(config, column_name, *func.GetChildren()[0]);
		if (parent_name.empty()) {
			return string();
		}
		auto &struct_type = func.GetChildren()[0]->GetReturnType();
		if (struct_type.id() != LogicalTypeId::STRUCT || StructType::IsUnnamed(struct_type)) {
			return string();
		}
		auto child_name = query::QueryWriter::WriteQuotedAndEscaped(
		    config.identifier_config, StructType::GetChildName(struct_type, child_idx).GetIdentifierName());
		return "(" + parent_name + ")." + child_name;
	}
	default:
		return string();
	}
}

string FilterPushdown::TransformFilterExpression(const FilterPushdown::Config &config, const string &column_name,
                                                 const Expression &expr) {
	if (BoundComparisonExpression::IsComparison(expr)) {
		auto &comparison = expr.Cast<BoundFunctionExpression>();
		auto comparison_type = comparison.GetExpressionType();
		auto &left = BoundComparisonExpression::Left(comparison);
		auto &right = BoundComparisonExpression::Right(comparison);
		auto subject = TransformExpressionSubject(config, column_name, left);
		const Value *constant = nullptr;
		if (!subject.empty() && right.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			constant = &right.Cast<BoundConstantExpression>().GetValue();
		} else {
			subject = TransformExpressionSubject(config, column_name, right);
			if (!subject.empty() && left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
				constant = &left.Cast<BoundConstantExpression>().GetValue();
				comparison_type = FlipComparisonExpression(comparison_type);
			}
		}
		if (!constant || subject.empty()) {
			return string();
		}
		return WriteConstantFilter(config, subject, comparison_type, *constant);
	}

	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		switch (conjunction.GetExpressionType()) {
		case ExpressionType::CONJUNCTION_AND:
			return WriteConjunction(config, column_name, conjunction.GetChildren(), "AND");
		case ExpressionType::CONJUNCTION_OR:
			return WriteConjunction(config, column_name, conjunction.GetChildren(), "OR");
		default:
			return string();
		}
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op = expr.Cast<BoundOperatorExpression>();
		if (op.GetChildren().empty()) {
			return string();
		}
		auto subject = TransformExpressionSubject(config, column_name, *op.GetChildren()[0]);
		if (subject.empty()) {
			return string();
		}
		switch (op.GetExpressionType()) {
		case ExpressionType::OPERATOR_IS_NULL:
			return WriteIsNull(subject, op);
		case ExpressionType::OPERATOR_IS_NOT_NULL:
			return WriteIsNotNull(subject, op);
		case ExpressionType::COMPARE_IN:
			return WriteCompareIn(config, subject, op);
		default:
			return string();
		}
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (func.Function().GetName() == OptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<OptionalFilterFunctionData>();
			return data.child_filter_expr ? TransformFilterExpression(config, column_name, *data.child_filter_expr)
			                              : string();
		}
		if (func.Function().GetName() == SelectivityOptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<SelectivityOptionalFilterFunctionData>();
			return data.child_filter_expr ? TransformFilterExpression(config, column_name, *data.child_filter_expr)
			                              : string();
		}
		if (func.Function().GetName() == DynamicFilterScalarFun::NAME) {
			return string();
		}

		switch (expr.GetExpressionType()) {
		case ExpressionType::COMPARE_EQUAL:
		case ExpressionType::COMPARE_NOTEQUAL:
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		case ExpressionType::COMPARE_DISTINCT_FROM:
		case ExpressionType::COMPARE_NOT_DISTINCT_FROM: {
			return TransformComparison(config, column_name, expr);
		}
		default:
			return string();
		}
	}
	default:
		return string();
	}
}

string FilterPushdown::TransformFilter(const FilterPushdown::Config &config, const string &column_name,
                                       const TableFilter &filter, column_t column_id) {
	if (IsVirtualColumn(column_id)) {
		return "FALSE";
	}
	string column_name_quoted = query::QueryWriter::WriteQuotedAndEscaped(config.identifier_config, column_name);
	auto &expr = FilterUtil::GetExpression(filter, "FilterPushdown::TransformFilter");
	return TransformFilterExpression(config, column_name_quoted, expr);
}

} // namespace table_scan
} // namespace dbconnector
