package com.exasol.adapter.dialects.teradata;

import com.exasol.adapter.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.metadata.*;
import com.exasol.adapter.sql.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;
import org.mockito.*;

import java.sql.*;
import java.util.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static utils.SqlNodesCreator.createSqlStatementSelect;

class TeradataSqlGenerationVisitorTest {
    private TeradataSqlGenerationVisitor visitor;
    @Mock
    private Connection connectionMock;

    @BeforeEach
    void beforeEach() {
        final SqlDialect dialect = new TeradataSqlDialect(this.connectionMock, AdapterProperties.emptyProperties());
        final SqlGenerationContext context = new SqlGenerationContext("test_catalog", "test_schema", false);
        this.visitor = new TeradataSqlGenerationVisitor(dialect, context);
    }

    @Test
    void testVisitSqlSelectListAnyValue() throws AdapterException {
        final SqlSelectList sqlSelectList = SqlSelectList.createAnyValueSelectList();
        assertThat(visitor.visit(sqlSelectList), equalTo("1"));
    }

    @Test
    void testVisitSqlSelectListSelectStar() throws AdapterException {
        final SqlSelectList sqlSelectList = SqlSelectList.createSelectStarSelectList();
        final SqlNode sqlStatementSelect = createSqlStatementSelect(sqlSelectList, Collections.EMPTY_LIST,
                "test_table");
        sqlSelectList.setParent(sqlStatementSelect);
        assertThat(visitor.visit(sqlSelectList), equalTo("*"));
    }

    @Test
    void testVisitSqlStatementSelect() throws AdapterException {
        final SqlStatementSelect select = (SqlStatementSelect) DialectTestData.getTestSqlNode();
        assertThat(visitor.visit(select), //
                equalTo("SELECT TOP 10 \"USER_ID\", COUNT(\"URL\") " //
                        + "FROM \"test_schema\".\"CLICKS\" " //
                        + "WHERE 1 < \"USER_ID\" " //
                        + "GROUP BY \"USER_ID\" HAVING 1 < COUNT(\"URL\") " //
                        + "ORDER BY \"USER_ID\""));
    }

    @CsvSource(value = { "SYSUDTLIB.ST_GEOMETRY : CAST(\"test_column\"  as VARCHAR(32000) )", //
            "XML : 'XMLSERIALIZE(DOCUMENT \"test_column\" as VARCHAR(32000) INCLUDING XMLDECLARATION) '", //
            "JSON : CAST(\"test_column\"  as VARCHAR(32000) )", //
            "TIME : CAST(\"test_column\"  as VARCHAR(21) )", //
            "TIME WITH TIME ZONE : CAST(\"test_column\"  as VARCHAR(21) )", //
            "CLOB : CAST(\"test_column\"  as VARCHAR(32000) )", //
            "INTERVAL : CAST(\"test_column\"  as VARCHAR(30) )", //
            "PERIOD : CAST(\"test_column\"  as VARCHAR(100) )" //
    }, delimiter = ':')
    @ParameterizedTest
    void testVisitSqlSelectListSelectStarRequiresCast(final String typeName, final String expected)
            throws AdapterException {
        final SqlSelectList sqlSelectList = SqlSelectList.createSelectStarSelectList();
        final List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(ColumnMetadata.builder().name("test_column")
                .adapterNotes("{\"jdbcDataType\":2009, \"typeName\":\"" + typeName + "\"}")
                .type(DataType.createVarChar(10, DataType.ExaCharset.UTF8)).build());
        final SqlNode sqlStatementSelect = createSqlStatementSelect(sqlSelectList, columns, "test_table");
        sqlSelectList.setParent(sqlStatementSelect);
        assertThat(visitor.visit(sqlSelectList), equalTo(expected));
    }

    @CsvSource({ "BYTE", //
            "VARBYTE", //
            "SYSUDTLIB", //
            "BLOB" //
    })
    @ParameterizedTest
    void testVisitSqlSelectListSelectStarUnsupportedType(final String typeName) throws AdapterException {
        final SqlSelectList sqlSelectList = SqlSelectList.createSelectStarSelectList();
        final List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(ColumnMetadata.builder().name("test_column")
                .adapterNotes("{\"jdbcDataType\":2009, \"typeName\":\"" + typeName + "\"}")
                .type(DataType.createVarChar(10, DataType.ExaCharset.UTF8)).build());
        final SqlNode sqlStatementSelect = createSqlStatementSelect(sqlSelectList, columns, "test_table");
        sqlSelectList.setParent(sqlStatementSelect);
        assertThat(visitor.visit(sqlSelectList), equalTo("'" + typeName + " NOT SUPPORTED'"));
    }

    @Test
    void testVisitSqlSelectListSelectStarThrowsException() {
        final SqlSelectList sqlSelectList = SqlSelectList.createSelectStarSelectList();
        final List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(ColumnMetadata.builder().name("test_column")
                .type(DataType.createVarChar(10, DataType.ExaCharset.UTF8)).build());
        final SqlNode sqlStatementSelect = createSqlStatementSelect(sqlSelectList, columns, "test_table");
        sqlSelectList.setParent(sqlStatementSelect);
        assertThrows(SqlGenerationVisitorException.class, () -> visitor.visit(sqlSelectList));
    }
}