#pragma once

#include <stdexcept>

namespace dbconnector {
namespace attached {

class AttachedException : public std::exception {
protected:
	std::string message;

public:
	AttachedException() = default;

	AttachedException(const std::string &message) : message(message.data(), message.length()) {
	}

	virtual const char *what() const noexcept {
		return message.c_str();
	}
};

} // namespace attached
} // namespace dbconnector
