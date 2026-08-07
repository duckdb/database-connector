#pragma once

#include <mutex>

#include "duckdb/common/reference_map.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

namespace dbconnector {
namespace storage {

template <typename CatalogT, typename TransactionT>
class TransactionManager : public duckdb::TransactionManager {
public:
	TransactionManager(duckdb::AttachedDatabase &db_p, CatalogT &catalog_p)
	    : duckdb::TransactionManager(db_p), catalog(catalog_p) {
	}

	duckdb::Transaction &StartTransaction(duckdb::ClientContext &context) override {
		auto transaction = duckdb::make_uniq<TransactionT>(catalog, *this, context);
		transaction->Start();
		auto &result = *transaction;
		std::lock_guard<std::mutex> guard(transaction_lock);
		transactions[result] = std::move(transaction);
		return result;
	}

	duckdb::ErrorData CommitTransaction(duckdb::ClientContext &context, duckdb::Transaction &transaction) override {
		auto &db_transaction = transaction.Cast<TransactionT>();
		duckdb::ErrorData error;
		try {
			db_transaction.Commit();
		} catch (const std::exception &ex) {
			error = duckdb::ErrorData(ex);
		} catch (...) {
			error = duckdb::ErrorData("Server COMMIT failed");
		}
		DestroyTransaction(transaction);
		return error;
	}

	void RollbackTransaction(duckdb::Transaction &transaction) override {
		auto &db_transaction = transaction.Cast<TransactionT>();
		try {
			db_transaction.Rollback();
		} catch (...) {
			DestroyTransaction(transaction);
			throw;
		}
		DestroyTransaction(transaction);
	}

	void Checkpoint(duckdb::ClientContext &context, bool force = false) override {
		auto &transaction = TransactionT::Get(context, db.GetCatalog());
		auto &conn = transaction.GetConnection();
		conn.Execute(context, "CHECKPOINT");
	}

protected:
	void DestroyTransaction(duckdb::Transaction &transaction) {
		std::lock_guard<std::mutex> guard(transaction_lock);
		transactions.erase(transaction);
	}

	CatalogT &catalog;
	std::mutex transaction_lock;
	duckdb::reference_map_t<duckdb::Transaction, duckdb::unique_ptr<TransactionT>> transactions;
};

} // namespace storage
} // namespace dbconnector
