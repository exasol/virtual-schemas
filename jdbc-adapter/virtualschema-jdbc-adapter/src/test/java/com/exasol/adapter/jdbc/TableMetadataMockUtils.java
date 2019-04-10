package com.exasol.adapter.jdbc;

import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;

public final class TableMetadataMockUtils {
    public static final String COLUMN_A1 = "TABLE_A_COLUMN_1";
    public static final String COLUMN_B1 = "TABLE_B_COLUMN_1";
    public static final String TABLE_B = "TABLE_B";
    public static final String TABLE_A = "TABLE_A";
    public static final String TABLE_A_COMMENT = "TABLE_A comment";
    public static final String TABLE_B_COMMENT = "TABLE_B comment";

    private TableMetadataMockUtils() {
        // prevent instantiation
    }

    public static void mockTableName(final ResultSet tablesMock, final String tableName, final String... moreTableNames)
            throws SQLException {
        when(tablesMock.getString(BaseTableMetadataReader.NAME_COLUMN)).thenReturn(tableName, moreTableNames);
    }

    public static void mockTableComment(final ResultSet tablesMock, final String comment, final String... moreComments)
            throws SQLException {
        when(tablesMock.getString(BaseTableMetadataReader.REMARKS_COLUMN)).thenReturn(comment, moreComments);
    }

    public static void mockTableWithColumnsOfType(final ResultSet tablesMock,
            final ColumnMetadataReader columnMetadataReaderMock, final String tableName, final DataType... types) {
        final List<ColumnMetadata> tableColumnMetadata = new ArrayList<>();
        int i = 1;
        for (final DataType type : types) {
            tableColumnMetadata.add(ColumnMetadata.builder().name(tableName + "_COLUMN_" + i).type(type).build());
            ++i;
        }
        when(columnMetadataReaderMock.mapColumns(tableName)).thenReturn(tableColumnMetadata);
    }

    public static void mockTableCount(final ResultSet tablesMock, final int count) throws SQLException {
        if (count == 0) {
            when(tablesMock.next()).thenReturn(false);
        } else {
            final Boolean[] nextResults = new Boolean[count];
            for (int i = 0; i < (count - 1); ++i) {
                nextResults[i] = true;
            }
            nextResults[count - 1] = false;
            when(tablesMock.next()).thenReturn(true, nextResults);
        }
    }
}