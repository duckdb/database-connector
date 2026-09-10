#pragma once

#include <string>

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"

#include "dbconnector/attached/attached_catalog.hpp"
#include "dbconnector/attached/attached_exception.hpp"

namespace dbconnector {
namespace attached {

class AttachedTable {
	AttachedCatalog attached_catalog;
	duckdb::optional_ptr<duckdb::TableCatalogEntry> table;

public:
	AttachedTable() {
	}

	explicit operator bool() const noexcept;

	bool operator!() const noexcept;

	template <typename T>
	T &Get() {
		if (!table) {
			throw AttachedException("Attached catalog table is not initialized");
		}
		return table->Cast<T>();
	}

	AttachedCatalog &GetCatalog();

	static AttachedTable Lookup(duckdb::ClientContext &ctx, const std::string &catalog_type,
	                            const duckdb::QualifiedName &name);

private:
	AttachedTable(AttachedCatalog attached_catalog, duckdb::TableCatalogEntry &table);
};

} // namespace attached
} // namespace dbconnector
