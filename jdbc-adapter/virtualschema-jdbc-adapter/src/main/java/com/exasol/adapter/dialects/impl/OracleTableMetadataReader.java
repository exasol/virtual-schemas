package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.BaseTableMetadataReader;
import com.exasol.adapter.jdbc.ColumnMetadataReader;

/**
 * This class implements a reader for Oracle database metadata
 */
public class OracleTableMetadataReader extends BaseTableMetadataReader {
    private static final String TRASH_BIN_TABLE_NAME_PREFIX = "BIN$";

    public OracleTableMetadataReader(final ColumnMetadataReader columnMetadataReader,
            final AdapterProperties properties) {
        super(columnMetadataReader, properties);
    }

    @Override
    public boolean isTableIncludedByMapping(final String tableName) {
        return !isTableInTrashBin(tableName);
    }

    private boolean isTableInTrashBin(final String tableName) {
        return tableName.startsWith(TRASH_BIN_TABLE_NAME_PREFIX);
    }
}