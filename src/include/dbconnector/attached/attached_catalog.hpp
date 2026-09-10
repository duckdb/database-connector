#pragma once

#include <string>

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"

#include "dbconnector/attached/attached_exception.hpp"

namespace dbconnector {
namespace attached {

class AttachedCatalog {
	duckdb::shared_ptr<duckdb::AttachedDatabase> database;
	duckdb::optional_ptr<duckdb::Catalog> catalog;

public:
	AttachedCatalog() {
	}

	explicit operator bool() const noexcept;

	bool operator!() const noexcept;

	template <typename T>
	T &Get() {
		if (!catalog) {
			throw AttachedException("Attached catalog is not initialized");
		}
		return catalog->Cast<T>();
	}

	static AttachedCatalog Lookup(duckdb::ClientContext &ctx, const std::string &catalog_type,
	                              const duckdb::Identifier &name);

private:
	AttachedCatalog(duckdb::shared_ptr<duckdb::AttachedDatabase> database, duckdb::Catalog &catalog);
};

} // namespace attached
} // namespace dbconnector
