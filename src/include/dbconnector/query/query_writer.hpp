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
	};

	static Config CreateConfig(char quote, QuoteEscapeStyle escape_style);
	static std::string WriteQuotedAndEscaped(const QueryWriter::Config &config, const std::string &text);
	static std::string WriteConstant(const duckdb::Value &val);

private:
	static std::string EncodeBlob(const std::string &val);
};

} // namespace query
} // namespace dbconnector
