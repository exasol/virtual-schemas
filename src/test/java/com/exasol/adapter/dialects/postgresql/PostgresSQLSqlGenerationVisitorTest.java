package com.exasol.adapter.dialects.postgresql;

import static com.exasol.adapter.dialects.VisitorAssertions.assertSqlNodeConvertedToAsterisk;
import static com.exasol.adapter.dialects.VisitorAssertions.assertSqlNodeConvertedToOne;
import static com.exasol.adapter.sql.ScalarFunction.POSIX_TIME;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.*;

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
import com.exasol.adapter.metadata.*;
import com.exasol.adapter.sql.*;

@ExtendWith(MockitoExtension.class)
class PostgresSQLSqlGenerationVisitorTest {
    private SqlGenerationVisitor visitor;

    @BeforeEach
    void beforeEach(@Mock final ConnectionFactory connectionFactoryMock) {
        final SqlDialect dialect = new PostgreSQLSqlDialect(connectionFactoryMock, AdapterProperties.emptyProperties());
        final SqlGenerationContext context = new SqlGenerationContext("test_catalog", "test_schema", false);
        this.visitor = new PostgresSQLSqlGenerationVisitor(dialect, context);
    }

    @CsvSource({ "ADD_DAYS, day", //
            "ADD_HOURS, hour", //
            "ADD_MINUTES, minute", //
            "ADD_SECONDS, second", //
            "ADD_YEARS, year", //
            "ADD_WEEKS, week" })
    @ParameterizedTest
    void testVisitSqlFunctionScalarAddDate(final ScalarFunction scalarFunction, final String expected)
            throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarForDateTest(scalarFunction, 10);
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo("\"test_column\" +  interval '10 " + expected + "'"));
    }

    private SqlFunctionScalar createSqlFunctionScalarForDateTest(final ScalarFunction scalarFunction,
            final int numericValue) {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlColumn(1,
                ColumnMetadata.builder().name("test_column")
                        .adapterNotes("{\"jdbcDataType\":93, " + "\"typeName\":\"TIMESTAMP\"}")
                        .type(DataType.createChar(20, DataType.ExaCharset.UTF8)).build()));
        arguments.add(new SqlLiteralExactnumeric(new BigDecimal(numericValue)));
        return new SqlFunctionScalar(scalarFunction, arguments);
    }

    @CsvSource({ "SECONDS_BETWEEN, SECOND", //
            "MINUTES_BETWEEN, MINUTE", //
            "HOURS_BETWEEN, HOUR", //
            "DAYS_BETWEEN, DAY", //
            "MONTHS_BETWEEN, MONTH", //
            "YEARS_BETWEEN, YEAR" })
    @ParameterizedTest
    void testVisitSqlFunctionScalarTimeBetween(final ScalarFunction scalarFunction, final String expected)
            throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarForDateTest(scalarFunction, 10);
        assertThat(this.visitor.visit(sqlFunctionScalar),
                equalTo("DATE_PART('" + expected + "', AGE(10,\"test_column\"))"));
    }

    @CsvSource({ "SECOND, SECOND", //
            "MINUTE, MINUTE", //
            "DAY, DAY", //
            "WEEK, WEEK", //
            "MONTH, MONTH", //
            "YEAR, YEAR" })
    @ParameterizedTest
    void testVisitSqlFunctionScalarDatetime(final ScalarFunction scalarFunction, final String expected)
            throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarForDateTest(scalarFunction, 0);
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo("DATE_PART('" + expected + "',\"test_column\")"));
    }

    @Test
    void testVisitSqlFunctionScalarPosixTime() throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarForDateTest(POSIX_TIME, 0);
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo("EXTRACT(EPOCH FROM \"test_column\")"));
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

    @CsvSource({ "varbit, VARCHAR", //
            "point, VARCHAR", //
            "line, VARCHAR", //
            "lseg, VARCHAR", //
            "box, VARCHAR", //
            "path, VARCHAR", //
            "polygon, VARCHAR", //
            "circle, VARCHAR", //
            "cidr, VARCHAR", //
            "citext, VARCHAR", //
            "inet, VARCHAR", //
            "macaddr, VARCHAR", //
            "interval, VARCHAR", //
            "json, VARCHAR", //
            "jsonb, VARCHAR", //
            "uuid, VARCHAR", //
            "tsquery, VARCHAR", //
            "tsvector, VARCHAR", //
            "xml, VARCHAR", //
            "smallserial, SMALLINT", //
            "serial, INTEGER", //
            "bigserial, BIGINT" //
    })
    @ParameterizedTest
    void testVisitSqlSelectListSelectStarRequiresCast(final String typeName, final String expectedCastType)
            throws AdapterException {
        final SqlSelectList sqlSelectList = createSqlSelectStarListWithOneColumn(
                "{\"jdbcDataType\":2009, \"typeName\":\"" + typeName + "\"}",
                DataType.createVarChar(10, DataType.ExaCharset.UTF8));
        assertThat(this.visitor.visit(sqlSelectList), equalTo("CAST(\"test_column\"  as " + expectedCastType + " )"));
    }

    @Test
    void testVisitSqlSelectListSelectStarUnsupportedType() throws AdapterException {
        final SqlSelectList sqlSelectList = createSqlSelectStarListWithOneColumn(
                "{\"jdbcDataType\":2009, \"typeName\":\"bytea\"}",
                DataType.createVarChar(10, DataType.ExaCharset.UTF8));
        assertThat(this.visitor.visit(sqlSelectList),
                equalTo("cast('bytea NOT SUPPORTED' as varchar) as not_supported"));
    }

    @Test
    void testVisitSqlStatementSelect() throws AdapterException {
        final SqlStatementSelect select = (SqlStatementSelect) DialectTestData.getTestSqlNode();
        assertThat(this.visitor.visit(select), //
                equalTo("SELECT \"user_id\", " //
                        + "COUNT(\"url\") FROM \"test_schema\".\"clicks\" " //
                        + "WHERE 1 < \"user_id\" " //
                        + "GROUP BY \"user_id\" " //
                        + "HAVING 1 < COUNT(\"url\") " //
                        + "ORDER BY \"user_id\" LIMIT 10"));
    }

    @Test
    void testVisitSqlSelectListSelectStarThrowsException() {
        final SqlSelectList sqlSelectList = createSqlSelectStarListWithOneColumn("",
                DataType.createVarChar(10, DataType.ExaCharset.UTF8));
        assertThrows(SqlGenerationVisitorException.class, () -> this.visitor.visit(sqlSelectList));
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

    @Test
    void testVisitSqlFunctionAggregateGroupConcat() throws AdapterException {
        final SqlLiteralString argument = new SqlLiteralString("test");
        final ColumnMetadata columnMetadata = ColumnMetadata.builder().name("test_column").type(DataType.createBool())
                .build();
        final ColumnMetadata columnMetadata2 = ColumnMetadata.builder().name("test_column2")
                .type(DataType.createDouble()).build();
        final List<SqlNode> orderByArguments = List.of(new SqlColumn(1, columnMetadata),
                new SqlColumn(2, columnMetadata2));
        final SqlOrderBy orderBy = new SqlOrderBy(orderByArguments, List.of(false, true), List.of(false, true));
        final SqlFunctionAggregateGroupConcat sqlFunctionAggregateGroupConcat = SqlFunctionAggregateGroupConcat
                .builder(argument).separator(new SqlLiteralString("'")).orderBy(orderBy).build();
        assertThat(this.visitor.visit(sqlFunctionAggregateGroupConcat), equalTo("STRING_AGG(E'test', E'''') "));
    }
}