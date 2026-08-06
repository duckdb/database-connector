#include "dbconnector/functions/configure_pool.hpp"

namespace dbconnector {
namespace functions {

using namespace duckdb;

static Value Lookup(const named_parameter_map_t &map, const std::string &key) {
	auto it = map.find(Identifier(key));
	if (it == map.end()) {
		return Value();
	}
	return it->second;
}

static std::pair<std::string, bool> LookupString(const named_parameter_map_t &map, const std::string &key) {
	Value val = Lookup(map, key);
	if (val.IsNull()) {
		return std::make_pair("", true);
	}
	std::string str = StringValue::Get(val);
	return std::make_pair(std::move(str), false);
}

static std::pair<uint64_t, bool> LookupUBigInt(const named_parameter_map_t &map, const std::string &key) {
	Value val = Lookup(map, key);
	if (val.IsNull()) {
		return std::make_pair(0, true);
	}
	uint64_t num = UBigIntValue::Get(val);
	return std::make_pair(num, false);
}

static std::pair<bool, bool> LookupBool(const named_parameter_map_t &map, const std::string &key) {
	Value val = Lookup(map, key);
	if (val.IsNull()) {
		return std::make_pair(false, true);
	}
	bool flag = BooleanValue::Get(val);
	return std::make_pair(flag, false);
}

static std::pair<dbconnector::pool::AcquireMode, bool> LookupAcquireMode(const named_parameter_map_t &map,
                                                                         const std::string &key) {
	std::pair<std::string, bool> st_pair = LookupString(map, key);
	if (st_pair.second) {
		return std::make_pair(dbconnector::pool::AcquireMode::FORCE, true);
	}
	try {
		dbconnector::pool::AcquireMode mode = dbconnector::pool::AcquireModeHelpers::FromString(st_pair.first);
		return std::make_pair(mode, false);
	} catch (const std::exception &e) {
		throw BinderException(e.what());
	}
}

ConfigurePool::BindData::BindData(const named_parameter_map_t &map)
    : catalog_name(LookupString(map, "catalog_name")), acquire_mode(LookupAcquireMode(map, "acquire_mode")),
      max_connections(LookupUBigInt(map, "max_connections")),
      wait_timeout_millis(LookupUBigInt(map, "wait_timeout_millis")),
      enable_thread_local_cache(LookupBool(map, "enable_thread_local_cache")),
      max_lifetime_millis(LookupUBigInt(map, "max_lifetime_millis")),
      idle_timeout_millis(LookupUBigInt(map, "idle_timeout_millis")),
      enable_reaper_thread(LookupBool(map, "enable_reaper_thread")),
      health_check_query(LookupString(map, "health_check_query")) {
	if (catalog_name.second &&
	    !(acquire_mode.second && max_connections.second && wait_timeout_millis.second &&
	      enable_thread_local_cache.second && max_lifetime_millis.second && idle_timeout_millis.second &&
	      enable_reaper_thread.second && health_check_query.second)) {
		throw BinderException("'catalog_name' argument must be specified to change any option value on the "
		                      "connection pool of this catalog");
	}
}

static void AddColumn(vector<LogicalType> &return_types, vector<string> &names, const std::string &col_name,
                      LogicalType col_type) {
	names.emplace_back(col_name);
	return_types.emplace_back(col_type);
}

unique_ptr<FunctionData> ConfigurePool::Bind(ClientContext &ctx, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<std::string> &names) {
	AddColumn(return_types, names, "catalog_name", LogicalType::VARCHAR);
	AddColumn(return_types, names, "acquire_mode", LogicalType::VARCHAR);
	AddColumn(return_types, names, "available_connections", LogicalType::UBIGINT);
	AddColumn(return_types, names, "total_connections", LogicalType::UBIGINT);
	AddColumn(return_types, names, "max_connections", LogicalType::UBIGINT);
	AddColumn(return_types, names, "wait_timeout_millis", LogicalType::UBIGINT);
	AddColumn(return_types, names, "cache_hits", LogicalType::UBIGINT);
	AddColumn(return_types, names, "cache_misses", LogicalType::UBIGINT);
	AddColumn(return_types, names, "try_failures", LogicalType::UBIGINT);
	AddColumn(return_types, names, "thread_local_cache_enabled", LogicalType::BOOLEAN);
	AddColumn(return_types, names, "thread_local_cache_hits", LogicalType::UBIGINT);
	AddColumn(return_types, names, "thread_local_cache_misses", LogicalType::UBIGINT);
	AddColumn(return_types, names, "max_lifetime_millis", LogicalType::UBIGINT);
	AddColumn(return_types, names, "idle_timeout_millis", LogicalType::UBIGINT);
	AddColumn(return_types, names, "reaper_thread_running", LogicalType::BOOLEAN);
	AddColumn(return_types, names, "reaper_thread_period_millis", LogicalType::UBIGINT);
	AddColumn(return_types, names, "health_check_query", LogicalType::VARCHAR);

	return make_uniq<BindData>(input.named_parameters);
}

unique_ptr<GlobalTableFunctionState> ConfigurePool::InitGlobalState(ClientContext &ctx, TableFunctionInitInput &input) {
	return make_uniq<GlobalState>();
}

unique_ptr<LocalTableFunctionState> ConfigurePool::InitLocalState(ExecutionContext &ctx, TableFunctionInitInput &input,
                                                                  GlobalTableFunctionState *globa_state) {
	return make_uniq<LocalState>();
}

named_parameter_type_map_t ConfigurePool::NamedParameters() {
	named_parameter_type_map_t res;
	res["catalog_name"] = LogicalType::VARCHAR;
	res["acquire_mode"] = LogicalType::VARCHAR;
	res["max_connections"] = LogicalType::UBIGINT;
	res["wait_timeout_millis"] = LogicalType::UBIGINT;
	res["enable_thread_local_cache"] = LogicalType::BOOLEAN;
	res["max_lifetime_millis"] = LogicalType::UBIGINT;
	res["idle_timeout_millis"] = LogicalType::UBIGINT;
	res["enable_reaper_thread"] = LogicalType::BOOLEAN;
	res["health_check_query"] = LogicalType::VARCHAR;
	return res;
}

} // namespace functions
} // namespace dbconnector
