#pragma once

#include <cstdint>
#include <string>
#include <utility>

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"

#include "dbconnector/pool.hpp"

namespace dbconnector {
namespace functions {

struct ConfigurePool {
	enum class ExecState { UNINITIALIZED, EXHAUSTED };

	struct BindData : public duckdb::TableFunctionData {
		std::pair<std::string, bool> catalog_name;
		std::pair<dbconnector::pool::AcquireMode, bool> acquire_mode;
		std::pair<uint64_t, bool> max_connections;
		std::pair<uint64_t, bool> wait_timeout_millis;
		std::pair<bool, bool> enable_thread_local_cache;
		std::pair<uint64_t, bool> max_lifetime_millis;
		std::pair<uint64_t, bool> idle_timeout_millis;
		std::pair<bool, bool> enable_reaper_thread;
		std::pair<std::string, bool> health_check_query;

		BindData(const duckdb::named_parameter_map_t &map);
	};

	struct GlobalState : public duckdb::GlobalTableFunctionState {};

	struct LocalState : public duckdb::LocalTableFunctionState {
		ExecState exec_state = ExecState::UNINITIALIZED;
	};

	static duckdb::unique_ptr<duckdb::FunctionData> Bind(duckdb::ClientContext &ctx,
	                                                     duckdb::TableFunctionBindInput &input,
	                                                     duckdb::vector<duckdb::LogicalType> &return_types,
	                                                     duckdb::vector<std::string> &names);

	static duckdb::unique_ptr<duckdb::GlobalTableFunctionState> InitGlobalState(duckdb::ClientContext &ctx,
	                                                                            duckdb::TableFunctionInitInput &input);

	static duckdb::unique_ptr<duckdb::LocalTableFunctionState>
	InitLocalState(duckdb::ExecutionContext &ctx, duckdb::TableFunctionInitInput &input,
	               duckdb::GlobalTableFunctionState *globa_state);
	static duckdb::named_parameter_type_map_t NamedParameters();

	template <typename ConnectionT, auto GetConnnectionPoolFromCatalog>
	static void Function(duckdb::ClientContext &ctx, duckdb::TableFunctionInput &input, duckdb::DataChunk &output) {
		using namespace duckdb;

		auto &bdata = input.bind_data->Cast<BindData>();
		auto &lstate = input.local_state->Cast<LocalState>();

		if (lstate.exec_state == ExecState::EXHAUSTED) {
			output.SetChildCardinality(0);
			return;
		}

		// collect pools
		std::vector<std::string> cat_names;
		std::vector<shared_ptr<pool::ConnectionPool<ConnectionT>>> pools;
		auto databases = DatabaseManager::Get(ctx).GetDatabases(ctx);
		for (auto &db_ref : databases) {
			auto &db = *db_ref;
			auto &catalog = db.GetCatalog();
			if (!bdata.catalog_name.second && catalog.GetName() != Identifier(bdata.catalog_name.first)) {
				continue;
			}
			shared_ptr<pool::ConnectionPool<ConnectionT>> pool = GetConnnectionPoolFromCatalog(catalog);
			if (pool) {
				cat_names.push_back(catalog.GetName().GetIdentifierName());
				pools.emplace_back(std::move(pool));
			}
		}

		if (!bdata.catalog_name.second && pools.size() == 0) {
			throw InvalidInputException("Catalog not found, name: '%s'", bdata.catalog_name.first);
		}

		// configure the single pool if specified
		if (!bdata.catalog_name.second && pools.size() > 0) {
			auto &pool = pools.at(0);
			if (!bdata.acquire_mode.second) {
				pool->SetAcquireMode(bdata.acquire_mode.first);
			}
			if (!bdata.max_connections.second) {
				pool->SetMaxConnections(bdata.max_connections.first);
			}
			if (!bdata.wait_timeout_millis.second) {
				pool->SetWaitTimeoutMillis(bdata.wait_timeout_millis.first);
			}
			if (!bdata.enable_thread_local_cache.second) {
				pool->SetThreadLocalCacheEnabled(bdata.enable_thread_local_cache.first);
			}
			if (!bdata.max_lifetime_millis.second) {
				pool->SetMaxLifetimeMillis(bdata.max_lifetime_millis.first);
			}
			if (!bdata.idle_timeout_millis.second) {
				pool->SetIdleTimeoutMillis(bdata.idle_timeout_millis.first);
			}
			if (!bdata.enable_reaper_thread.second) {
				if (bdata.enable_reaper_thread.first) {
					pool->EnsureReaperRunning();
				} else {
					pool->ShutdownReaper();
				}
			}
			if (!bdata.health_check_query.second) {
				pool->SetHealthCheckQuery(bdata.health_check_query.first);
			}
		}

		// set results
		idx_t row_idx = 0;
		for (auto &pool : pools) {
			idx_t col_idx = 0;
			output.data[col_idx++].SetValue(row_idx, Value(cat_names.at(row_idx)));
			output.data[col_idx++].SetValue(row_idx, Value(pool::AcquireModeHelpers::ToString(pool->GetAcquireMode())));
			output.data[col_idx++].SetValue(row_idx, Value::UBIGINT(pool->GetAvailableConnections()));
			output.data[col_idx++].SetValue(row_idx, Value::UBIGINT(pool->GetTotalConnections()));
			output.data[col_idx++].SetValue(row_idx, Value::UBIGINT(pool->GetMaxConnections()));
			output.data[col_idx++].SetValue(row_idx, Value::UBIGINT(pool->GetWaitTimeoutMillis()));
			output.data[col_idx++].SetValue(row_idx, Value::UBIGINT(pool->GetCacheHits()));
			output.data[col_idx++].SetValue(row_idx, Value::UBIGINT(pool->GetCacheMisses()));
			output.data[col_idx++].SetValue(row_idx, Value::UBIGINT(pool->GetTryFailures()));
			output.data[col_idx++].SetValue(row_idx, Value::BOOLEAN(pool->IsThreadLocalCacheEnabled()));
			output.data[col_idx++].SetValue(row_idx, Value::UBIGINT(pool->GetThreadLocalCacheHits()));
			output.data[col_idx++].SetValue(row_idx, Value::UBIGINT(pool->GetThreadLocalCacheMisses()));
			output.data[col_idx++].SetValue(row_idx, Value::UBIGINT(pool->GetMaxLifetimeMillis()));
			output.data[col_idx++].SetValue(row_idx, Value::UBIGINT(pool->GetIdleTimeoutMillis()));
			output.data[col_idx++].SetValue(row_idx, Value::BOOLEAN(pool->IsReaperRunning()));
			output.data[col_idx++].SetValue(row_idx, Value::UBIGINT(pool->GetReaperPeriodMillis()));
			output.data[col_idx++].SetValue(row_idx, Value(pool->GetHealthCheckQuery()));
			row_idx++;
		}

		output.SetChildCardinality(row_idx);
		lstate.exec_state = ExecState::EXHAUSTED;
	}
};

} // namespace functions
} // namespace dbconnector
