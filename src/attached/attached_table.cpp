#include "dbconnector/attached/attached_table.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"

namespace dbconnector {
namespace attached {

using namespace duckdb;

AttachedTable::AttachedTable(AttachedCatalog attached_catalog_p, TableCatalogEntry &table_p)
    : attached_catalog(std::move(attached_catalog_p)), table(&table_p) {
}

AttachedTable::operator bool() const noexcept {
	return table != nullptr;
}

bool AttachedTable::operator!() const noexcept {
	return !static_cast<bool>(*this);
}

AttachedCatalog &AttachedTable::GetCatalog() {
	return attached_catalog;
}

AttachedTable AttachedTable::Lookup(duckdb::ClientContext &ctx, const std::string &catalog_type,
                                    const duckdb::QualifiedName &name) {
	AttachedCatalog attached_catalog = AttachedCatalog::Lookup(ctx, catalog_type, name.Catalog());
	if (!attached_catalog) {
		return AttachedTable();
	}
	Catalog &catalog = attached_catalog.Get<Catalog>();

	CatalogTransaction catalog_transaction(catalog, ctx);
	QualifiedName schema_name(name.Schema());
	EntryLookupInfo schema_lookup(CatalogType::SCHEMA_ENTRY, std::move(schema_name));
	optional_ptr<SchemaCatalogEntry> schema_ptr =
	    catalog.LookupSchema(catalog_transaction, schema_lookup, OnEntryNotFound::RETURN_NULL);
	if (!schema_ptr) {
		return AttachedTable();
	}
	SchemaCatalogEntry &schema = *schema_ptr;

	QualifiedName table_name(name.Name());
	EntryLookupInfo table_lookup(CatalogType::TABLE_ENTRY, std::move(table_name));
	optional_ptr<CatalogEntry> table_ptr = schema.LookupEntry(catalog_transaction, table_lookup);
	if (!table_ptr) {
		return AttachedTable();
	}
	TableCatalogEntry &table = table_ptr->Cast<TableCatalogEntry>();

	return AttachedTable(std::move(attached_catalog), table);
}

} // namespace attached
} // namespace dbconnector
