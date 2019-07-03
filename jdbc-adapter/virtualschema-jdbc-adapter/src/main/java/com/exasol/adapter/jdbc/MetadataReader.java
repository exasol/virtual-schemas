package com.exasol.adapter.jdbc;

/**
 * Common interface for all metadata readers.
 */
public interface MetadataReader {
    /**
     * Get the catalog name that is applied as filter criteria when looking up remote metadata.
     *
     * @return catalog name or <code>null</code> if metadata lookups are not limited by catalog
     */
    String getCatalogNameFilter();

    /**
     * Get the schema name that is applied as filter criteria when looking up remote metadata.
     *
     * @return schema name or <code>null</code> if metadata lookups are not limited by schema
     */
    String getSchemaNameFilter();
}