package com.exasol.adapter.jdbc;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;

/**
 * This class contains parts that are used commonly across all types of metadata readers.
 *
 * @see BaseRemoteMetadataReader
 * @see BaseTableMetadataReader
 * @see BaseColumnMetadataReader
 */
public abstract class AbstractMetadataReader implements MetadataReader {
    protected final AdapterProperties properties;
    protected final Connection connection;

    /**
     * Create an {@link AbstractMetadataReader}.
     *
     * @param connection JDBC connection to remote data source
     * @param properties user-defined adapter properties
     */
    public AbstractMetadataReader(final Connection connection, final AdapterProperties properties) {
        this.properties = properties;
        this.connection = connection;
    }

    /**
     * Get the catalog name that is applied as filter criteria when looking up remote metadata.
     *
     * @return catalog name or <code>null</code> if metadata lookups are not limited by catalog
     */
    @Override
    public String getCatalogNameFilter() {
        return this.properties.getCatalogName();
    }

    /**
     * Get the schema name that is applied as filter criteria when looking up remote metadata.
     *
     * @return schema name or <code>null</code> if metadata lookups are not limited by schema
     */
    @Override
    public String getSchemaNameFilter() {
        return this.properties.getSchemaName();
    }
}