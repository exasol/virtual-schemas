package com.exasol.adapter.jdbc;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.BaseIdentifierConverter;
import com.exasol.adapter.dialects.IdentifierConverter;

/**
 * This class implements basic reading of database metadata from JDBC.
 *
 * <p>
 * See <a href=
 * "https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html">java.sql.DatabaseMetaData</a>
 */
public final class BaseRemoteMetadataReader extends AbstractRemoteMetadataReader {
    /**
     * Create a new instance of a {@link BaseTableMetadataReader}.
     *
     * @param connection database connection through which the reader retrieves the metadata from the remote source
     * @param properties user-defined properties
     */
    public BaseRemoteMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    /**
     * Create a reader that handles column metadata.
     * <p>
     * Override this method in cases where a remote data source needs specific handling of column metadata
     *
     * @return column metadata reader
     */
    @Override
    protected ColumnMetadataReader createColumnMetadataReader() {
        return new BaseColumnMetadataReader(this.connection, this.properties, this.identifierConverter);
    }

    /**
     * Create a reader that handles table metadata.
     * <p>
     * Override this method in cases where a remote data source needs specific handling of table metadata
     *
     * @return table metadata reader
     */
    @Override
    protected TableMetadataReader createTableMetadataReader() {
        return new BaseTableMetadataReader(this.connection, this.columnMetadataReader, this.properties,
                this.identifierConverter);
    }

    /**
     * Create a converter that translates identifiers from the remote data source to the Exasol representation.
     *
     * @return identifier converter
     */
    @Override
    protected IdentifierConverter createIdentifierConverter() {
        return BaseIdentifierConverter.createDefault();
    }
}