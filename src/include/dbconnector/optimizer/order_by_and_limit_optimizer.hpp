#pragma once

#include <string>

#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

namespace dbconnector {
namespace optimizer {

class OrderByAndLimitOptimizer {
public:
	struct Config {
		bool enabled = false;
		char identifier_quote = '"';
		std::string table_scan_name;
	};

	static Config CreateConfig(duckdb::ClientContext &ctx, const std::string &enabled_option, char identifier_quote,
	                           std::string table_scan_name);

	static void Optimize(const Config &config, duckdb::OptimizerExtensionInput &input,
	                     duckdb::unique_ptr<duckdb::LogicalOperator> &op);
};

} // namespace optimizer
} // namespace dbconnector
