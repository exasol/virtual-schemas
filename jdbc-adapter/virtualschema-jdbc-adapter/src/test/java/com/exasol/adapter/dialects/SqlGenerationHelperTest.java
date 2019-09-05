package com.exasol.adapter.dialects;

import static com.exasol.adapter.dialects.SqlGenerationHelper.addMetadata;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.exasol.adapter.metadata.*;
import com.exasol.adapter.sql.*;

class SqlGenerationHelperTest {
    private static final String TABLE_NAME_1 = "TABLE_NAME_1";
    private static final String TABLE_NAME_2 = "TABLE_NAME_2";
    private static final String TABLE_NAME_3 = "TABLE_NAME_3";
    private static final String TABLE_NAME_4 = "TABLE_NAME_4";
    private List<ColumnMetadata> columns;

    @BeforeEach
    void setUp() {
        this.columns = new ArrayList<>();
        this.columns.add(ColumnMetadata.builder().name("column1").type(DataType.createBool()).build());
    }

    @Test
    void testGetMetadataFromTable() {
        final TableMetadata metadataT1 = new TableMetadata(TABLE_NAME_1, "", this.columns, "");
        final SqlNode t1 = new SqlTable(TABLE_NAME_1, metadataT1);
        final List<TableMetadata> allMetadata = new ArrayList<>();
        addMetadata(t1, allMetadata);
        assertEquals(1, allMetadata.size());
        assertEquals(TABLE_NAME_1, allMetadata.get(0).getName());
    }

    @Test
    void testGetMetadataFromSimpleJoin() {
        final TableMetadata metadataT1 = new TableMetadata(TABLE_NAME_1, "", this.columns, "");
        final TableMetadata metadataT2 = new TableMetadata(TABLE_NAME_2, "", this.columns, "");

        final SqlNode t1 = new SqlTable(TABLE_NAME_1, metadataT1);
        final SqlNode t2 = new SqlTable(TABLE_NAME_2, metadataT2);
        final SqlNode join = new SqlJoin(t1, t2, Mockito.mock(SqlPredicateEqual.class), JoinType.INNER);

        final List<TableMetadata> allMetadata = new ArrayList<>();
        addMetadata(join, allMetadata);
        assertEquals(2, allMetadata.size());
        assertEquals(TABLE_NAME_1, allMetadata.get(0).getName());
        assertEquals(TABLE_NAME_2, allMetadata.get(1).getName());
    }

    @Test
    void testGetMetadataFromNestedJoin() {
        final TableMetadata metadataT1 = new TableMetadata(TABLE_NAME_1, "", this.columns, "");
        final TableMetadata metadataT2 = new TableMetadata(TABLE_NAME_2, "", this.columns, "");
        final TableMetadata metadataT3 = new TableMetadata(TABLE_NAME_3, "", this.columns, "");

        final SqlNode table1 = new SqlTable(TABLE_NAME_1, metadataT1);
        final SqlNode table2 = new SqlTable(TABLE_NAME_2, metadataT2);
        final SqlNode table3 = new SqlTable(TABLE_NAME_3, metadataT3);
        final SqlNode joinT1T2 = new SqlJoin(table1, table2, Mockito.mock(SqlPredicateEqual.class), JoinType.INNER);
        final SqlNode join = new SqlJoin(joinT1T2, table3, Mockito.mock(SqlPredicateEqual.class), JoinType.INNER);

        final List<TableMetadata> allMetadata = new ArrayList<>();
        addMetadata(join, allMetadata);
        assertEquals(3, allMetadata.size());
        assertEquals(TABLE_NAME_1, allMetadata.get(0).getName());
        assertEquals(TABLE_NAME_2, allMetadata.get(1).getName());
        assertEquals(TABLE_NAME_3, allMetadata.get(2).getName());
    }

    @Test
    void testGetMetadataFromTwoNestedJoins() {
        final TableMetadata metadataT1 = new TableMetadata(TABLE_NAME_1, "", this.columns, "");
        final TableMetadata metadataT2 = new TableMetadata(TABLE_NAME_2, "", this.columns, "");
        final TableMetadata metadataT3 = new TableMetadata(TABLE_NAME_3, "", this.columns, "");
        final TableMetadata metadataT4 = new TableMetadata(TABLE_NAME_4, "", this.columns, "");

        final SqlNode t1 = new SqlTable(TABLE_NAME_1, metadataT1);
        final SqlNode t2 = new SqlTable(TABLE_NAME_2, metadataT2);
        final SqlNode t3 = new SqlTable(TABLE_NAME_3, metadataT3);
        final SqlNode t4 = new SqlTable(TABLE_NAME_4, metadataT4);
        final SqlNode joinT1T2 = new SqlJoin(t1, t2, Mockito.mock(SqlPredicateEqual.class), JoinType.INNER);
        final SqlNode joinT3T4 = new SqlJoin(t3, t4, Mockito.mock(SqlPredicateEqual.class), JoinType.INNER);
        final SqlNode join = new SqlJoin(joinT1T2, joinT3T4, Mockito.mock(SqlPredicateEqual.class), JoinType.INNER);

        final List<TableMetadata> allMetadata = new ArrayList<>();
        addMetadata(join, allMetadata);
        assertEquals(4, allMetadata.size());
        assertEquals(TABLE_NAME_1, allMetadata.get(0).getName());
        assertEquals(TABLE_NAME_2, allMetadata.get(1).getName());
        assertEquals(TABLE_NAME_3, allMetadata.get(2).getName());
        assertEquals(TABLE_NAME_4, allMetadata.get(3).getName());
    }
}