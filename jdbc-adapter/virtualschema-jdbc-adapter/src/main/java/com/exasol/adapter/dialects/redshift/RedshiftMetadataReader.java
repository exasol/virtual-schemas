package com.exasol.adapter.dialects.redshift;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.*;

/**
 * This class implements a Redshift-specific metadata reader
 */
public class RedshiftMetadataReader extends BaseRemoteMetadataReader {
    /**
     * Create a new instance of a {@link RedshiftMetadataReader}
     *
     * @param connection           JDBC connection to the remote data source
     * @param columnMetadataReader reader to be used to map the metadata of the tables columns
     * @param properties           user-defined adapter properties
     * @param identifierConverter  converter between source and Exasol identifiers
     */
    public RedshiftMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    protected ColumnMetadataReader createColumnMetadataReader() {
        return new RedshiftColumnMetadataReader(this.connection, this.properties, getIdentifierConverter());
    }

    @Override
    protected TableMetadataReader createTableMetadataReader() {
        return new RedshiftTableMetadataReader(this.connection, getColumnMetadataReader(), this.properties,
                getIdentifierConverter());
    }
}