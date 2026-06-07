#include "dbconnector/query/query_writer.hpp"

#include <cstdint>

namespace dbconnector {
namespace query {

QueryWriter::Config QueryWriter::CreateConfig(char quote, QuoteEscapeStyle escape_style,
                                              const std::string &blob_literal_prefix,
                                              const std::string &blob_literal_suffix) {
	Config res;
	res.quote = quote;
	res.escape_style = escape_style;
	res.blob_literal_prefix = blob_literal_prefix;
	res.blob_literal_suffix = blob_literal_suffix;
	return res;
}

std::string QueryWriter::WriteQuotedAndEscaped(const QueryWriter::Config &config, const std::string &text) {
	std::string result;
	result.reserve(text.length() + 2);
	result.push_back(config.quote);
	for (auto c : text) {
		if (c == config.quote) {
			switch (config.escape_style) {
			case QuoteEscapeStyle::BACKSLASH:
				result.push_back('\\');
				break;
			case QuoteEscapeStyle::DOUBLE_QUOTE:
				result.push_back(config.quote);
				break;
			default:
				throw QueryWriterException("Invalid unsupported escape style specified: " +
				                           std::to_string(static_cast<int>(config.escape_style)));
			}
			result.push_back(config.quote);
		} else if (c == '\\' && config.escape_style == QuoteEscapeStyle::BACKSLASH) {
			result.push_back('\\');
			result.push_back('\\');
		} else {
			result.push_back(c);
		}
	}
	result.push_back(config.quote);
	return result;
}

std::string QueryWriter::EncodeBlob(const QueryWriter::Config &config, const std::string &val) {
	char const HEX_DIGITS[] = "0123456789ABCDEF";

	std::string result = config.blob_literal_prefix;
	for (size_t i = 0; i < val.size(); i++) {
		uint8_t byte_val = static_cast<uint8_t>(val[i]);
		result += HEX_DIGITS[(byte_val >> 4) & 0xf];
		result += HEX_DIGITS[byte_val & 0xf];
	}
	result += "'";
	result += config.blob_literal_suffix;
	return result;
}

std::string QueryWriter::WriteConstant(const QueryWriter::Config &config, const duckdb::Value &val) {
	if (val.type().IsNumeric() || val.type().id() == duckdb::LogicalTypeId::BOOLEAN) {
		return val.ToSQLString();
	}
	if (val.type().id() == duckdb::LogicalTypeId::BLOB) {
		return EncodeBlob(config, duckdb::StringValue::Get(val));
	}
	if (val.type().id() == duckdb::LogicalTypeId::TIMESTAMP_TZ) {
		return val.DefaultCastAs(duckdb::LogicalType::TIMESTAMP)
		    .DefaultCastAs(duckdb::LogicalType::VARCHAR)
		    .ToSQLString();
	}
	return val.DefaultCastAs(duckdb::LogicalType::VARCHAR).ToSQLString();
}

} // namespace query
} // namespace dbconnector
