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
public abstract class AbstractMetadataReader {
    protected final Connection connection;
    protected final AdapterProperties properties;

    /**
     * Create an {@link AbstractMetadataReader}
     *
     * @param properties user-defined adapter properties
     */
    public AbstractMetadataReader(final Connection connection, final AdapterProperties properties) {
        this.connection = connection;
        this.properties = properties;
    }

    /**
     * Get the catalog in which the database metadata to be read resides
     *
     * @return parent catalog for metadata to be read
     */
    protected String getCatalogName() {
        return this.properties.getCatalogName();
    }

    /**
     * Get the schema in which the database metadata to be read resides
     *
     * @return parent schema for metadata to be read
     */
    protected String getSchemaName() {
        return this.properties.getSchemaName();
    }
}