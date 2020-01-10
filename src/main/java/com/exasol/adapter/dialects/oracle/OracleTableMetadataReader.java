package com.exasol.adapter.dialects.oracle;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.jdbc.BaseTableMetadataReader;
import com.exasol.adapter.jdbc.ColumnMetadataReader;

/**
 * This class implements a reader for Oracle database metadata.
 */
public class OracleTableMetadataReader extends BaseTableMetadataReader {
    private static final String TRASH_BIN_TABLE_NAME_PREFIX = "BIN$";

    /**
     * Create a new {@link OracleTableMetadataReader} instance.
     *
     * @param connection           connection to the remote data source
     * @param columnMetadataReader reader to be used to map the metadata of the tables columns
     * @param properties           user-defined adapter properties
     * @param identifierConverter  converter between source and Exasol identifiers
     */
    public OracleTableMetadataReader(final Connection connection, final ColumnMetadataReader columnMetadataReader,
            final AdapterProperties properties, final IdentifierConverter identifierConverter) {
        super(connection, columnMetadataReader, properties, identifierConverter);
    }

    @Override
    public boolean isTableIncludedByMapping(final String tableName) {
        return !isTableInTrashBin(tableName);
    }

    private boolean isTableInTrashBin(final String tableName) {
        return tableName.startsWith(TRASH_BIN_TABLE_NAME_PREFIX);
    }
}