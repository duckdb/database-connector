#pragma once

#include "duckdb/function/function.hpp"

#include "dbconnector/optimizer/aggregate_bind_data.hpp"
#include "dbconnector/optimizer/order_by_and_limit_bind_data.hpp"

namespace dbconnector {

class BindData : public duckdb::FunctionData {
public:
	virtual optimizer::AggregateBindData &GetAggregateBindData() = 0;

	virtual optimizer::OrderByAndLimitBindData &GetOrderByAndLimitBindData() = 0;
};

} // namespace dbconnector
