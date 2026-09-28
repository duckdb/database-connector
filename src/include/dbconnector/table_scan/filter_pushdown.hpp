#pragma once

#include <string>

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/planner/table_filter.hpp"

#include "dbconnector/query/query_writer.hpp"

namespace dbconnector {
namespace table_scan {

//! Where a constant sorts relative to every value a MySQL column can contain. MySQL has no
//! infinite dates / timestamps and no inf / nan doubles, so DuckDB's non-finite constants
//! compare above (infinity, nan) or below (-infinity) every value in the column.
enum class FilterConstantRange { FINITE, ABOVE_ALL_VALUES, BELOW_ALL_VALUES };

using write_distinct_from_t = std::string (*)(duckdb::ExpressionType distict_type, const std::string &column,
                                              const std::string &constant);

using get_constant_range_t = FilterConstantRange (*)(const duckdb::Value &constant);

using write_non_finite_comparizon_t = std::string (*)(const std::string &column_name,
                                                      duckdb::ExpressionType comparison_type,
                                                      FilterConstantRange range);

class FilterPushdown {
public:
	struct Config {
		query::QueryWriter::Config identifier_config;
		query::QueryWriter::Config constant_config;
		std::string varchar_comparison_collation;
		write_distinct_from_t write_distinct_from = nullptr;
		get_constant_range_t get_constant_range = nullptr;
		write_non_finite_comparizon_t write_non_finite_comparizon = nullptr;
	};

	static Config CreateConfig(char identifier_quote, char constant_quote, query::QuoteEscapeStyle escape_style,
	                           const std::string &blob_literal_prefix = std::string(),
	                           const std::string &blob_literal_suffix = std::string(),
	                           const std::string &varchar_comparison_collation = std::string(),
	                           write_distinct_from_t write_distinct_from = nullptr,
	                           get_constant_range_t get_constant_range = nullptr,
	                           write_non_finite_comparizon_t write_non_finite_comparizon = nullptr);

	static std::string TransformFilter(const Config &config, const std::string &column_name,
	                                   const duckdb::TableFilter &filter, duckdb::column_t column_id);

	static std::string TransformFilterExpression(const FilterPushdown::Config &config, const std::string &column_name,
	                                             const duckdb::Expression &expr);
};

} // namespace table_scan
} // namespace dbconnector
