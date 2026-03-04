#pragma once

#include <stdexcept>

namespace dbconnector {
namespace pool {

class PoolException : public std::exception {
protected:
	std::string message;

public:
	PoolException() = default;

	PoolException(const std::string &message) : message(message.data(), message.length()) {
	}

	virtual const char *what() const noexcept {
		return message.c_str();
	}
};

} // namespace pool
} // namespace dbconnector
