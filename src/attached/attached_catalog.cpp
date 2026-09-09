#include "dbconnector/attached/attached_catalog.hpp"

#include "duckdb/catalog/catalog.hpp"

namespace dbconnector {
namespace attached {

using namespace duckdb;

AttachedCatalog::AttachedCatalog(shared_ptr<AttachedDatabase> database_p, Catalog &catalog_p)
    : database(std::move(database_p)), catalog(&catalog_p) {
}

AttachedCatalog::operator bool() const noexcept {
	return catalog != nullptr;
}

bool AttachedCatalog::operator!() const noexcept {
	return !static_cast<bool>(*this);
}

AttachedCatalog AttachedCatalog::Lookup(ClientContext &ctx, const std::string &catalog_type, const std::string &name) {
	vector<shared_ptr<AttachedDatabase>> databases = DatabaseManager::Get(ctx).GetDatabases(ctx);
	for (shared_ptr<AttachedDatabase> &db_ptr : databases) {
		AttachedDatabase &db = *db_ptr;
		Catalog &catalog = db.GetCatalog();
		if (catalog.GetCatalogType() == catalog_type && catalog.GetName() == name) {
			return AttachedCatalog(db_ptr, catalog);
		}
	}
	return AttachedCatalog();
}

} // namespace attached
} // namespace dbconnector
