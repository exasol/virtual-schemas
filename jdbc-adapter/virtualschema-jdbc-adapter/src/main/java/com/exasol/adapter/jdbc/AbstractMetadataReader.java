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
    static final String ANY_CATALOG_FILTER = null;
    public ColumnMetadataReader columnMetadataReader;
    protected Connection connection;
    protected final AdapterProperties properties;

    @Override
    public IdentifierCaseHandling getUnquotedIdentifierHandling() {
        return IdentifierCaseHandling.INTERPRET_AS_UPPER;
    }

    @Override
    public IdentifierCaseHandling getQuotedIdentifierHandling() {
        return IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE;
    }

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
     * Create an {@link AbstractMetadataReader}
     *
     * @param properties user-defined adapter properties
     */
    public AbstractMetadataReader(final ColumnMetadataReader columnMetadataReader, final AdapterProperties properties) {
        this.columnMetadataReader = columnMetadataReader;
        this.properties = properties;
    }

    protected String changeIdentifierCaseIfNeeded(final String identifier) {
        if (getQuotedIdentifierHandling() == getUnquotedIdentifierHandling()) {
            if (getQuotedIdentifierHandling() != IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE) {
                return identifier.toUpperCase();
            }
        }
        return identifier;
    }

    /**
     * Get the catalog name that is applied as filter criteria when looking up remote metadata
     *
     * @return catalog name or <code>null</code> if metadata lookups are not limited by catalog
     */
    @Override
    public String getCatalogNameFilter() {
        return this.properties.getCatalogName();
    }

    /**
     * Get the schema name that is applied as filter criteria when looking up remote metadata
     *
     * @return schema name or <code>null</code> if metadata lookups are not limited by schema
     */
    @Override
    public String getSchemaNameFilter() {
        return this.properties.getSchemaName();
    }
}