package com.exasol.adapter.dialects;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.MetadataException;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.sql.JoinType;
import com.exasol.adapter.sql.SqlJoin;
import com.exasol.adapter.sql.SqlNode;
import com.exasol.adapter.sql.SqlPredicateEqual;
import com.exasol.adapter.sql.SqlTable;

import org.junit.Test;
import org.mockito.Mockito;


public class SqlGenerationHelperTest {

    @Test
    public void testGetMetadataFromTable() throws MetadataException {
        List<ColumnMetadata> columns = Arrays.asList(Mockito.mock(ColumnMetadata.class));
        TableMetadata metadataT1 = new TableMetadata("T1", "", columns, "");

        SqlNode t1 = new SqlTable("T1", metadataT1);

        SqlGenerationHelper helper = new SqlGenerationHelper();
        List<TableMetadata> allMetadata = new ArrayList<TableMetadata>();
		helper.getMetadataFrom(t1, allMetadata);
        assertEquals(allMetadata.size(), 1);
        assertEquals(allMetadata.get(0).getName(), "T1");
    }

    @Test
    public void testGetMetadataFromSimpleJoin() throws MetadataException {
        List<ColumnMetadata> columns = Arrays.asList(Mockito.mock(ColumnMetadata.class));
        TableMetadata metadataT1 = new TableMetadata("T1", "", columns, "");
        TableMetadata metadataT2 = new TableMetadata("T2", "", columns, "");

        SqlNode t1 = new SqlTable("T1", metadataT1);
        SqlNode t2 = new SqlTable("T2", metadataT2);
        SqlNode join = new SqlJoin(t1, t2, Mockito.mock(SqlPredicateEqual.class), JoinType.INNER);

        SqlGenerationHelper helper = new SqlGenerationHelper();
        List<TableMetadata> allMetadata = new ArrayList<TableMetadata>();
		helper.getMetadataFrom(join, allMetadata);
        assertEquals(allMetadata.size(), 2);
        assertEquals(allMetadata.get(0).getName(), "T1");
        assertEquals(allMetadata.get(1).getName(), "T2");
    }

    @Test
    public void testGetMetadataFromNestedJoin() throws MetadataException {
        List<ColumnMetadata> columns = Arrays.asList(Mockito.mock(ColumnMetadata.class));
        TableMetadata metadataT1 = new TableMetadata("T1", "", columns, "");
        TableMetadata metadataT2 = new TableMetadata("T2", "", columns, "");
        TableMetadata metadataT3 = new TableMetadata("T3", "", columns, "");

        SqlNode t1 = new SqlTable("T1", metadataT1);
        SqlNode t2 = new SqlTable("T2", metadataT2);
        SqlNode t3 = new SqlTable("T3", metadataT3);
        SqlNode joinT1T2 = new SqlJoin(t1, t2, Mockito.mock(SqlPredicateEqual.class), JoinType.INNER);
        SqlNode join = new SqlJoin(joinT1T2, t3, Mockito.mock(SqlPredicateEqual.class), JoinType.INNER);

        SqlGenerationHelper helper = new SqlGenerationHelper();
        List<TableMetadata> allMetadata = new ArrayList<TableMetadata>();
		helper.getMetadataFrom(join, allMetadata);
        assertEquals(allMetadata.size(), 3);
        assertEquals(allMetadata.get(0).getName(), "T1");
        assertEquals(allMetadata.get(1).getName(), "T2");
        assertEquals(allMetadata.get(2).getName(), "T3");
    }

    @Test
    public void testGetMetadataFromTwoNestedJoins() throws MetadataException {
        List<ColumnMetadata> columns = Arrays.asList(Mockito.mock(ColumnMetadata.class));
        TableMetadata metadataT1 = new TableMetadata("T1", "", columns, "");
        TableMetadata metadataT2 = new TableMetadata("T2", "", columns, "");
        TableMetadata metadataT3 = new TableMetadata("T3", "", columns, "");
        TableMetadata metadataT4 = new TableMetadata("T4", "", columns, "");

        SqlNode t1 = new SqlTable("T1", metadataT1);
        SqlNode t2 = new SqlTable("T2", metadataT2);
        SqlNode t3 = new SqlTable("T3", metadataT3);
        SqlNode t4 = new SqlTable("T3", metadataT4);
        SqlNode joinT1T2 = new SqlJoin(t1, t2, Mockito.mock(SqlPredicateEqual.class), JoinType.INNER);
        SqlNode joinT3T4 = new SqlJoin(t3, t4, Mockito.mock(SqlPredicateEqual.class), JoinType.INNER);
        SqlNode join = new SqlJoin(joinT1T2, joinT3T4, Mockito.mock(SqlPredicateEqual.class), JoinType.INNER);

        SqlGenerationHelper helper = new SqlGenerationHelper();
        List<TableMetadata> allMetadata = new ArrayList<TableMetadata>();
		helper.getMetadataFrom(join, allMetadata);
        assertEquals(allMetadata.size(), 4);
        assertEquals(allMetadata.get(0).getName(), "T1");
        assertEquals(allMetadata.get(1).getName(), "T2");
        assertEquals(allMetadata.get(2).getName(), "T3");
        assertEquals(allMetadata.get(3).getName(), "T4");
    }
}