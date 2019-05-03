package com.exasol.adapter.dialects.redshift;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.jdbc.BaseTableMetadataReader;
import com.exasol.adapter.jdbc.ColumnMetadataReader;

/**
 * This class implements a reader for Redshift database metadata
 */
public class RedshiftTableMetadataReader extends BaseTableMetadataReader {

    /**
     * Create a new {@link RedshiftTableMetadataReader} instance
     *
     * @param columnMetadataReader reader to be used to map the metadata of the tables columns
     * @param properties           user-defined adapter properties
     */
    public RedshiftTableMetadataReader(final Connection connection, final ColumnMetadataReader columnMetadataReader,
            final AdapterProperties properties, final IdentifierConverter identifierConverter) {
        super(connection, columnMetadataReader, properties, identifierConverter);
    }
}