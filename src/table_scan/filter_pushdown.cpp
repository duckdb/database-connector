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

FilterPushdown::Config FilterPushdown::CreateConfig(char identifier_quote, char constant_quote,
                                                    query::QuoteEscapeStyle escape_style,
                                                    const std::string &blob_literal_prefix,
                                                    const std::string &blob_literal_suffix) {
	Config res;
	res.identifier_quote = identifier_quote;
	res.constant_quote = constant_quote;
	res.escape_style = escape_style;
	res.blob_literal_prefix = blob_literal_prefix;
	res.blob_literal_suffix = blob_literal_suffix;
	return res;
}

std::string FilterPushdown::CreateExpression(const query::QueryWriter::Config &identifier_config,
                                             const query::QueryWriter::Config &constant_config,
                                             const std::string &column_name,
                                             const vector<unique_ptr<Expression>> &filters, const std::string &op,
                                             column_t column_id) {
	vector<std::string> filter_entries;
	for (auto &filter : filters) {
		auto new_filter = TransformExpression(identifier_config, constant_config, column_name, *filter, column_id);
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

string FilterPushdown::TransformConstantFilter(const query::QueryWriter::Config &constant_config,
                                               const string &column_name, ExpressionType comparison_type,
                                               const Value &constant, column_t column_id) {
	string constant_string;
	if (IsVirtualColumn(column_id)) {
		return "FALSE";
	} else {
		constant_string = query::QueryWriter::WriteConstant(constant_config, constant);
	}
	auto operator_string = TransformComparison(comparison_type);
	string comparison = StringUtil::Format("%s %s %s", column_name, operator_string, constant_string);
	if (constant.type().id() == LogicalTypeId::VARCHAR) {
		comparison += " COLLATE \"C\"";
	}
	return comparison;
}

string FilterPushdown::TransformExpressionSubject(const query::QueryWriter::Config &identifier_config,
                                                  const string &column_name, const Expression &expr) {
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
		auto parent_name = TransformExpressionSubject(identifier_config, column_name, *func.GetChildren()[0]);
		if (parent_name.empty()) {
			return string();
		}
		auto &struct_type = func.GetChildren()[0]->GetReturnType();
		if (struct_type.id() != LogicalTypeId::STRUCT || StructType::IsUnnamed(struct_type)) {
			return string();
		}
		auto child_name = query::QueryWriter::WriteQuotedAndEscaped(identifier_config,
		                                                            StructType::GetChildName(struct_type, child_idx));
		return "(" + parent_name + ")." + child_name;
	}
	default:
		return string();
	}
}

std::string FilterPushdown::TransformExpression(const query::QueryWriter::Config &identifier_config,
                                                const query::QueryWriter::Config &constant_config,
                                                const std::string &column_name, const Expression &expr,
                                                column_t column_id) {
	if (BoundComparisonExpression::IsComparison(expr)) {
		auto &comparison = expr.Cast<BoundFunctionExpression>();
		auto comparison_type = comparison.GetExpressionType();
		auto &left = BoundComparisonExpression::Left(comparison);
		auto &right = BoundComparisonExpression::Right(comparison);
		auto subject = TransformExpressionSubject(identifier_config, column_name, left);
		const Value *constant = nullptr;
		if (!subject.empty() && right.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			constant = &right.Cast<BoundConstantExpression>().GetValue();
		} else {
			subject = TransformExpressionSubject(identifier_config, column_name, right);
			if (!subject.empty() && left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
				constant = &left.Cast<BoundConstantExpression>().GetValue();
				comparison_type = FlipComparisonExpression(comparison_type);
			}
		}
		if (!constant || subject.empty()) {
			return string();
		}
		return TransformConstantFilter(constant_config, subject, comparison_type, *constant, column_id);
	}

	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		switch (conjunction.GetExpressionType()) {
		case ExpressionType::CONJUNCTION_AND:
			return CreateExpression(identifier_config, constant_config, column_name, conjunction.GetChildren(), "AND",
			                        column_id);
		case ExpressionType::CONJUNCTION_OR:
			return CreateExpression(identifier_config, constant_config, column_name, conjunction.GetChildren(), "OR",
			                        column_id);
		default:
			return std::string();
		}
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op = expr.Cast<BoundOperatorExpression>();
		auto subject = op.GetChildren().empty()
		                   ? string()
		                   : TransformExpressionSubject(identifier_config, column_name, *op.GetChildren()[0]);
		switch (op.GetExpressionType()) {
		case ExpressionType::OPERATOR_IS_NULL:
			if (!subject.empty()) {
				return subject + " IS NULL";
			}
			return std::string();
		case ExpressionType::OPERATOR_IS_NOT_NULL:
			if (!subject.empty()) {
				return subject + " IS NOT NULL";
			}
			return std::string();
		case ExpressionType::COMPARE_IN: {
			if (subject.empty()) {
				return string();
			}
			std::string in_list;
			for (idx_t i = 1; i < op.GetChildren().size(); i++) {
				if (op.GetChildren()[i]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
					return std::string();
				}
				if (!in_list.empty()) {
					in_list += ", ";
				}
				if (IsVirtualColumn(column_id)) {
					in_list += "FALSE";
				} else {
					in_list += query::QueryWriter::WriteConstant(
					    identifier_config, op.GetChildren()[i]->Cast<BoundConstantExpression>().GetValue());
				}
			}
			return IsVirtualColumn(column_id) ? "FALSE" : subject + " IN (" + in_list + ")";
		}
		default:
			return std::string();
		}
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (func.Function().GetName() == OptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<OptionalFilterFunctionData>();
			return data.child_filter_expr ? TransformExpression(identifier_config, constant_config, column_name,
			                                                    *data.child_filter_expr, column_id)
			                              : std::string();
		}
		if (func.Function().GetName() == SelectivityOptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<SelectivityOptionalFilterFunctionData>();
			return data.child_filter_expr ? TransformExpression(identifier_config, constant_config, column_name,
			                                                    *data.child_filter_expr, column_id)
			                              : std::string();
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
                                            const TableFilter &filter, column_t column_id) {
	auto identifier_config = query::QueryWriter::CreateConfig(config.identifier_quote, config.escape_style);
	auto constant_config = query::QueryWriter::CreateConfig(config.constant_quote, config.escape_style,
	                                                        config.blob_literal_prefix, config.blob_literal_suffix);
	std::string column_name_quoted = query::QueryWriter::WriteQuotedAndEscaped(identifier_config, column_name);
	auto &expr = FilterUtil::GetExpression(filter, "FilterPushdown::TransformFilter");
	return TransformExpression(identifier_config, constant_config, column_name_quoted, expr, column_id);
}

} // namespace table_scan
} // namespace dbconnector
