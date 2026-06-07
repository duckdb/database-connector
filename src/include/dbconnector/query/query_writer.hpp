#pragma once

#include <string>

#include "duckdb/common/types/value.hpp"

#include "dbconnector/query/query_writer_exception.hpp"

namespace dbconnector {
namespace query {

enum class QuoteEscapeStyle { BACKSLASH, DOUBLE_QUOTE };

class QueryWriter {
public:
	struct Config {
		char quote = '"';
		QuoteEscapeStyle escape_style = QuoteEscapeStyle::DOUBLE_QUOTE;
		std::string blob_literal_prefix;
		std::string blob_literal_suffix;
	};

	static Config CreateConfig(char quote, QuoteEscapeStyle escape_style,
	                           const std::string &blob_literal_prefix = std::string(),
	                           const std::string &blob_literal_suffix = std::string());
	static std::string WriteQuotedAndEscaped(const QueryWriter::Config &config, const std::string &text);
	static std::string WriteConstant(const QueryWriter::Config &config, const duckdb::Value &val);

private:
	static std::string EncodeBlob(const QueryWriter::Config &config, const std::string &val);
};

} // namespace query
} // namespace dbconnector
