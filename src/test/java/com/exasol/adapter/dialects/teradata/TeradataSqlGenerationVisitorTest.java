package com.exasol.adapter.dialects.teradata;

import static com.exasol.adapter.dialects.VisitorAssertions.assertSqlNodeConvertedToAsterisk;
import static com.exasol.adapter.dialects.VisitorAssertions.assertSqlNodeConvertedToOne;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.exasol.adapter.metadata.*;
import com.exasol.adapter.sql.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.ConnectionFactory;

import java.util.*;

@ExtendWith(MockitoExtension.class)
class TeradataSqlGenerationVisitorTest {
    private TeradataSqlGenerationVisitor visitor;

    @BeforeEach
    void beforeEach(@Mock final ConnectionFactory connectionFactoryMock) {
        final SqlDialect dialect = new TeradataSqlDialect(connectionFactoryMock, AdapterProperties.emptyProperties());
        final SqlGenerationContext context = new SqlGenerationContext("test_catalog", "test_schema", false);
        this.visitor = new TeradataSqlGenerationVisitor(dialect, context);
    }

    @Test
    void testVisitSqlSelectListAnyValue() throws AdapterException {
        final SqlSelectList sqlSelectList = SqlSelectList.createAnyValueSelectList();
        assertSqlNodeConvertedToOne(sqlSelectList, this.visitor);
    }

    @Test
    void testVisitSqlSelectListSelectStar() throws AdapterException {
        final SqlSelectList sqlSelectList = SqlSelectList.createSelectStarSelectList();
        final TableMetadata tableMetadata = new TableMetadata("", "", Collections.emptyList(), "");
        final SqlTable fromClause = new SqlTable("test_table", tableMetadata);
        final SqlNode sqlStatementSelect = SqlStatementSelect.builder().selectList(sqlSelectList).fromClause(fromClause)
                .build();
        sqlSelectList.setParent(sqlStatementSelect);
        assertSqlNodeConvertedToAsterisk(sqlSelectList, this.visitor);
    }

    @Test
    void testVisitSqlStatementSelect() throws AdapterException {
        final SqlStatementSelect select = (SqlStatementSelect) DialectTestData.getTestSqlNode();
        assertThat(this.visitor.visit(select), //
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
                DataType.createVarChar(10, DataType.ExaCharset.UTF8));
        assertThat(this.visitor.visit(sqlSelectList), equalTo(expected));
    }

    private SqlSelectList createSqlSelectStarListWithOneColumn(final String adapterNotes, final DataType dataType) {
        final SqlSelectList selectList = SqlSelectList.createSelectStarSelectList();
        final List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(ColumnMetadata.builder().name("test_column").adapterNotes(adapterNotes).type(dataType).build());
        final TableMetadata tableMetadata = new TableMetadata("", "", columns, "");
        final SqlTable fromClause = new SqlTable("", tableMetadata);
        final SqlNode sqlStatementSelect = SqlStatementSelect.builder().selectList(selectList).fromClause(fromClause)
                .build();
        selectList.setParent(sqlStatementSelect);
        return selectList;
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
                DataType.createVarChar(10, DataType.ExaCharset.UTF8));
        assertThat(this.visitor.visit(sqlSelectList), equalTo("'" + typeName + " NOT SUPPORTED'"));
    }

    @Test
    void testVisitSqlSelectListSelectStarThrowsException() {
        final SqlSelectList sqlSelectList = createSqlSelectStarListWithOneColumn("",
                DataType.createVarChar(10, DataType.ExaCharset.UTF8));
        assertThrows(SqlGenerationVisitorException.class, () -> this.visitor.visit(sqlSelectList));
    }
}