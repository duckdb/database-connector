#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"

#include "dbconnector/pool/connection_pool.hpp"
#include "dbconnector/pool/pooled_connection.hpp"

namespace dbconnector {
namespace connect {

template <typename ConnectionT>
class ConnectSessions;

//! Tracks the CONNECT sessions of a client, to close them once the client disconnects (or is destroyed while
//! CONNECT-ed). Registered in the client by ConnectSessions::GetConnection.
template <typename ConnectionT>
class ConnectSessionState : public duckdb::ClientContextState {
public:
	explicit ConnectSessionState(duckdb::connection_t client_id);
	~ConnectSessionState() override;

	void AddSession(duckdb::AttachedDatabase &db, std::weak_ptr<ConnectSessions<ConnectionT>> sessions);

	void StatementEnd(duckdb::ClientContext &context) override;
	void QueryEnd(duckdb::ClientContext &context) override;

private:
	struct Session {
		duckdb::weak_ptr<duckdb::AttachedDatabase> db;
		std::weak_ptr<ConnectSessions<ConnectionT>> sessions;
	};

	void CloseDisconnectedSessions(duckdb::ClientContext &context);
	void CloseSession(Session &session) noexcept;

	duckdb::connection_t client_id;
	std::vector<Session> sessions;
};

//! The CONNECT sessions of the clients of an attached database. Each client CONNECT-ed to the database runs all its
//! statements on a single connection, pinned in the pool of the database, so that the transactions the client issues
//! (BEGIN / COMMIT / ROLLBACK) and the session state span the statements.
//!
//! The catalog of the attached database owns this object (through a std::shared_ptr) and passes the ID returned by
//! GetConnection to the query function that runs the statement (as its pinned connection). The session of a client is
//! closed when the client disconnects (or is destroyed): the connection is returned to the pool, which resets it
//! (rolling back an open transaction). The sessions that are still open when this object is destroyed (the database
//! is detached) are closed without resetting the connections.
template <typename ConnectionT>
class ConnectSessions : public std::enable_shared_from_this<ConnectSessions<ConnectionT>> {
public:
	//! Acquires (and prepares) the connection of a new session
	using OpenFunction = std::function<pool::PooledConnection<ConnectionT>(duckdb::ClientContext &context)>;

	//! state_name is the key of the ConnectSessionState in the clients - it must be unique per extension
	ConnectSessions(pool::ConnectionPool<ConnectionT> &pool, std::string state_name);
	~ConnectSessions();

	ConnectSessions(const ConnectSessions &) = delete;
	ConnectSessions &operator=(const ConnectSessions &) = delete;

	//! Returns the ID of the pinned connection of the session of the client, opening a session with the
	//! connection returned by open if the client has none
	uint64_t GetConnection(duckdb::ClientContext &context, duckdb::AttachedDatabase &db, const OpenFunction &open);
	//! Same as above, the connection of a new session is acquired with ForceAcquire: the session does not wait for
	//! a free slot in the pool
	uint64_t GetConnection(duckdb::ClientContext &context, duckdb::AttachedDatabase &db);

	//! Closes the session of the client, if there is one: the connection is returned to the pool
	void Close(duckdb::connection_t client_id);

private:
	std::shared_ptr<pool::ConnectionPool<ConnectionT>> pool;
	std::string state_name;
	std::mutex sessions_lock;
	//! The IDs of the pinned connections, by the ID of the client
	std::unordered_map<duckdb::connection_t, uint64_t> sessions;
};

} // namespace connect
} // namespace dbconnector
