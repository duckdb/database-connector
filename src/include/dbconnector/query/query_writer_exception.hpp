#pragma once

#include <stdexcept>

namespace dbconnector {
namespace query {

class QueryWriterException : public std::exception {
protected:
	std::string message;

public:
	QueryWriterException() = default;

	QueryWriterException(const std::string &message) : message(message.data(), message.length()) {
	}

	virtual const char *what() const noexcept {
		return message.c_str();
	}
};

} // namespace query
} // namespace dbconnector
