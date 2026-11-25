#pragma once

#include "connect_sessions.hpp"

#include "dbconnector/pool/connection_pool_impl.hpp"
#include "dbconnector/pool/pooled_connection_impl.hpp"
#include "dbconnector/pool/thread_local_connection_cache_impl.hpp"

namespace dbconnector {
namespace connect {

template <typename ConnectionT>
ConnectSessionState<ConnectionT>::ConnectSessionState(duckdb::connection_t client_id_p) : client_id(client_id_p) {
}

template <typename ConnectionT>
ConnectSessionState<ConnectionT>::~ConnectSessionState() {
	// the client is destroyed while CONNECT-ed
	for (auto &session : sessions) {
		CloseSession(session);
	}
}

template <typename ConnectionT>
void ConnectSessionState<ConnectionT>::AddSession(duckdb::AttachedDatabase &db,
                                                  std::weak_ptr<ConnectSessions<ConnectionT>> sessions_p) {
	Session session;
	session.db = db.shared_from_this();
	session.sessions = std::move(sessions_p);
	sessions.push_back(std::move(session));
}

template <typename ConnectionT>
void ConnectSessionState<ConnectionT>::StatementEnd(duckdb::ClientContext &context) {
	CloseDisconnectedSessions(context);
}

template <typename ConnectionT>
void ConnectSessionState<ConnectionT>::QueryEnd(duckdb::ClientContext &context) {
	CloseDisconnectedSessions(context);
}

template <typename ConnectionT>
void ConnectSessionState<ConnectionT>::CloseDisconnectedSessions(duckdb::ClientContext &context) {
	if (sessions.empty()) {
		return;
	}
	auto connected = context.TryGetConnectedCatalog();
	for (size_t i = sessions.size(); i > 0; i--) {
		auto &session = sessions[i - 1];
		auto db = session.db.lock();
		if (db && db == connected) {
			continue;
		}
		CloseSession(session);
		sessions.erase(sessions.begin() + static_cast<std::ptrdiff_t>(i - 1));
	}
}

template <typename ConnectionT>
void ConnectSessionState<ConnectionT>::CloseSession(Session &session) noexcept {
	try {
		auto connect_sessions = session.sessions.lock();
		if (!connect_sessions) {
			// the database was detached, which closed the session
			return;
		}
		connect_sessions->Close(client_id);
	} catch (...) {
		// suppress
	}
}

template <typename ConnectionT>
ConnectSessions<ConnectionT>::ConnectSessions(pool::ConnectionPool<ConnectionT> &pool_p, std::string state_name_p)
    : pool(pool_p.shared_from_this()), state_name(std::move(state_name_p)) {
}

template <typename ConnectionT>
ConnectSessions<ConnectionT>::~ConnectSessions() {
	// the pinned connections reference the pool - close them, so that the pool can be destroyed
	std::lock_guard<std::mutex> guard(sessions_lock);
	for (auto &entry : sessions) {
		try {
			auto connection = pool->UnpinConnection(entry.second);
			// close the connection without resetting it for reuse
			connection.Invalidate();
		} catch (...) {
			// suppress
		}
	}
	sessions.clear();
}

template <typename ConnectionT>
uint64_t ConnectSessions<ConnectionT>::GetConnection(duckdb::ClientContext &context, duckdb::AttachedDatabase &db,
                                                     const OpenFunction &open) {
	auto client_id = context.GetConnectionId();
	{
		std::lock_guard<std::mutex> guard(sessions_lock);
		auto entry = sessions.find(client_id);
		if (entry != sessions.end()) {
			return entry->second;
		}
	}
	auto connection_id = pool->PinConnection(open(context));
	{
		std::lock_guard<std::mutex> guard(sessions_lock);
		sessions[client_id] = connection_id;
	}
	auto state =
	    context.registered_state->template GetOrCreate<ConnectSessionState<ConnectionT>>(state_name, client_id);
	state->AddSession(db, this->shared_from_this());
	return connection_id;
}

template <typename ConnectionT>
uint64_t ConnectSessions<ConnectionT>::GetConnection(duckdb::ClientContext &context, duckdb::AttachedDatabase &db) {
	auto &connection_pool = *pool;
	return GetConnection(context, db,
	                     [&connection_pool](duckdb::ClientContext &) { return connection_pool.ForceAcquire(); });
}

template <typename ConnectionT>
void ConnectSessions<ConnectionT>::Close(duckdb::connection_t client_id) {
	uint64_t connection_id;
	{
		std::lock_guard<std::mutex> guard(sessions_lock);
		auto entry = sessions.find(client_id);
		if (entry == sessions.end()) {
			return;
		}
		connection_id = entry->second;
		sessions.erase(entry);
	}
	// returning the connection to the pool resets it (e.g. rolls back an open transaction)
	auto connection = pool->UnpinConnection(connection_id);
}

} // namespace connect
} // namespace dbconnector
