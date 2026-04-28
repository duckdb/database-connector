#pragma once

#include <cctype>
#include <algorithm>
#include <string>

#include "dbconnector/pool/pool_exception.hpp"

namespace dbconnector {
namespace pool {

enum class AcquireMode { FORCE, WAIT, TRY };

struct AcquireModeHelpers {
	static AcquireMode FromString(const std::string &mode_str) {
		std::string ms(mode_str.data(), mode_str.length());
		std::transform(ms.begin(), ms.end(), ms.begin(),
		               [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
		if (ms == "force") {
			return AcquireMode::FORCE;
		} else if (ms == "wait") {
			return AcquireMode::WAIT;
		} else if (ms == "try") {
			return AcquireMode::TRY;
		} else {
			throw PoolException("Invalid unsupported acquire mode: '" + mode_str + "'");
		}
	}

	static std::string ToString(AcquireMode mode) {
		switch (mode) {
		case AcquireMode::FORCE:
			return "force";
		case AcquireMode::WAIT:
			return "wait";
		case AcquireMode::TRY:
			return "try";
		default:
			throw PoolException("Invalid unsupported acquire mode: d" + std::to_string(static_cast<int>(mode)));
		}
	}
};

} // namespace pool
} // namespace dbconnector
