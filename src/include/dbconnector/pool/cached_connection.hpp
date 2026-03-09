#pragma once

#include <chrono>
#include <memory>

namespace dbconnector {
namespace pool {

template <typename ConnectionT>
class CachedConnection {
	std::unique_ptr<ConnectionT> connection;
	std::chrono::steady_clock::time_point created_at;
	std::chrono::steady_clock::time_point returned_at;

public:
	CachedConnection() {
	}

	CachedConnection(std::unique_ptr<ConnectionT> connection_p, std::chrono::steady_clock::time_point created_at_p,
	                 std::chrono::steady_clock::time_point returned_at_p)
	    : connection(std::move(connection_p)), created_at(created_at_p), returned_at(returned_at_p) {
	}

	CachedConnection(const CachedConnection &) = delete;
	CachedConnection &operator=(const CachedConnection &) = delete;

	CachedConnection(CachedConnection &&other) = default;
	CachedConnection &operator=(CachedConnection &&other) = default;

	explicit operator bool() const {
		return connection.get() != nullptr;
	}

	void reset() {
		connection.reset();
	}

	ConnectionT &GetConnection() {
		return *connection;
	}

	std::unique_ptr<ConnectionT> TakeConnection() {
		return std::move(connection);
	}

	std::chrono::steady_clock::time_point GetCreatedAt() const {
		return created_at;
	}

	std::chrono::steady_clock::time_point GetReturnedAt() const {
		return returned_at;
	}
};

} // namespace pool
} // namespace dbconnector