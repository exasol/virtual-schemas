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

import static com.exasol.adapter.dialects.VisitorAssertions.assertSqlNodeConvertedToAsterisk;
import static com.exasol.adapter.dialects.VisitorAssertions.assertSqlNodeConvertedToOne;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static utils.SqlNodesCreator.*;

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
        assertSqlNodeConvertedToOne(sqlSelectList, visitor);
    }

    @Test
    void testVisitSqlSelectListSelectStar() throws AdapterException {
        final SqlSelectList sqlSelectList = createSqlSelectStarListWithoutColumns();
        assertSqlNodeConvertedToAsterisk(sqlSelectList, visitor);
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
        final SqlSelectList sqlSelectList = createSqlSelectStarListWithOneColumn(
                "{\"jdbcDataType\":2009, \"typeName\":\"" + typeName + "\"}",
                DataType.createVarChar(10, DataType.ExaCharset.UTF8), "test_column");
        assertThat(visitor.visit(sqlSelectList), equalTo(expected));
    }

    @CsvSource({ "BYTE", //
            "VARBYTE", //
            "SYSUDTLIB", //
            "BLOB" //
    })
    @ParameterizedTest
    void testVisitSqlSelectListSelectStarUnsupportedType(final String typeName) throws AdapterException {
        final SqlSelectList sqlSelectList = createSqlSelectStarListWithOneColumn(
                "{\"jdbcDataType\":2009, \"typeName\":\"" + typeName + "\"}",
                DataType.createVarChar(10, DataType.ExaCharset.UTF8), "test_column");
        assertThat(visitor.visit(sqlSelectList), equalTo("'" + typeName + " NOT SUPPORTED'"));
    }

    @Test
    void testVisitSqlSelectListSelectStarThrowsException() {
        final SqlSelectList sqlSelectList = createSqlSelectStarListWithOneColumn("",
                DataType.createVarChar(10, DataType.ExaCharset.UTF8), "test_column");
        assertThrows(SqlGenerationVisitorException.class, () -> visitor.visit(sqlSelectList));
    }
}