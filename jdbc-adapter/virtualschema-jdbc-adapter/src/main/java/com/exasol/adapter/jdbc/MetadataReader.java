package com.exasol.adapter.jdbc;

public interface MetadataReader {

    String ANY_CATALOG_FILTER = null;
    String ANY_SCHEMA_FILTER = null;

    /**
     * Get the catalog name that is applied as filter criteria when looking up remote metadata
     *
     * @return catalog name or <code>null</code> if metadata lookups are not limited by catalog
     */
    String getCatalogNameFilter();

    /**
     * Get the schema name that is applied as filter criteria when looking up remote metadata
     *
     * @return schema name or <code>null</code> if metadata lookups are not limited by schema
     */
    String getSchemaNameFilter();

}