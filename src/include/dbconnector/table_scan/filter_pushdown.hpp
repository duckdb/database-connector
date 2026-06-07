#pragma once

#include <string>

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/planner/table_filter.hpp"

#include "dbconnector/query/query_writer.hpp"

namespace dbconnector {
namespace table_scan {

class FilterPushdown {
	struct Config {
		char identifier_quote = '"';
		char constant_quote = '\'';
		query::QuoteEscapeStyle escape_style = query::QuoteEscapeStyle::DOUBLE_QUOTE;
		std::string blob_literal_prefix;
		std::string blob_literal_suffix;
	};

public:
	static Config CreateConfig(char identifier_quote, char constant_quote, query::QuoteEscapeStyle escape_style,
	                           const std::string &blob_literal_prefix = std::string(),
	                           const std::string &blob_literal_suffix = std::string());

	static std::string TransformFilter(const Config &config, const std::string &column_name,
	                                   const duckdb::TableFilter &filter, duckdb::column_t column_id);

private:
	static std::string TransformExpression(const query::QueryWriter::Config &identifier_config,
	                                       const query::QueryWriter::Config &constant_config,
	                                       const std::string &column_name, const duckdb::Expression &expr,
	                                       duckdb::column_t column_id);
	static std::string TransformExpressionSubject(const query::QueryWriter::Config &identifier_config,
	                                              const std::string &column_name, const duckdb::Expression &expr);
	static std::string TransformConstantFilter(const query::QueryWriter::Config &constant_config,
	                                           const std::string &column_name, duckdb::ExpressionType comparison_type,
	                                           const duckdb::Value &constant, duckdb::column_t column_id);
	static std::string TransformComparison(duckdb::ExpressionType type);
	static std::string CreateExpression(const query::QueryWriter::Config &identifier_config,
	                                    const query::QueryWriter::Config &constant_config,
	                                    const std::string &column_name,
	                                    const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &filters,
	                                    const std::string &op, duckdb::column_t column_id);
};

} // namespace table_scan
} // namespace dbconnector
