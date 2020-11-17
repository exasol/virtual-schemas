package com.exasol.adapter.dialects.sqlserver;

import static com.exasol.adapter.dialects.VisitorAssertions.assertSqlNodeConvertedToAsterisk;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.*;

import com.exasol.adapter.metadata.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.sql.*;

class SqlServerSqlGenerationVisitorTest {
    private SqlServerSqlGenerationVisitor visitor;

    @BeforeEach
    void beforeEach() {
        final SqlDialectFactory factory = new SqlServerSqlDialectFactory();
        final SqlDialect dialect = factory.createSqlDialect(null, AdapterProperties.emptyProperties());
        final SqlGenerationContext context = new SqlGenerationContext("test_catalog", "test_schema", false);
        this.visitor = new SqlServerSqlGenerationVisitor(dialect, context);
    }

    @Test
    void testVisitSqlSelectListAnyValue() throws AdapterException {
        final SqlSelectList sqlSelectList = SqlSelectList.createAnyValueSelectList();
        assertThat(this.visitor.visit(sqlSelectList), equalTo("true"));
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
    void testVisitSqlSelectListSelectStarRequiresCast() throws AdapterException {
        final SqlSelectList sqlSelectList = createSqlSelectStarListWithOneColumn(
                "{\"jdbcDataType\":-155, \"typeName\":\"datetimeoffset\"}",
                DataType.createVarChar(36, DataType.ExaCharset.UTF8));
        assertThat(this.visitor.visit(sqlSelectList), equalTo("CAST([test_column] as VARCHAR(34))"));
    }

    @Test
    void testVisitSqlStatementSelect() throws AdapterException {
        final SqlStatementSelect select = (SqlStatementSelect) DialectTestData.getTestSqlNode();
        assertThat(this.visitor.visit(select), //
                equalTo("SELECT TOP 10 [USER_ID], COUNT_BIG([URL])" //
                        + " FROM [test_catalog].[test_schema].[CLICKS]" //
                        + " WHERE 1 < [USER_ID]" //
                        + " GROUP BY [USER_ID] HAVING 1 < COUNT_BIG([URL])" //
                        + " ORDER BY [USER_ID] NULLS LAST"));
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

    @CsvSource({ "ADD_DAYS, DAY", //
            "ADD_HOURS, HOUR", //
            "ADD_MINUTES, MINUTE", //
            "ADD_SECONDS, SECOND", //
            "ADD_YEARS, YEAR", //
            "ADD_WEEKS, WEEK" })
    @ParameterizedTest
    void testVisitSqlFunctionScalarAddDate(final ScalarFunction scalarFunction, final String expected)
            throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarForDateTest(scalarFunction);
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo("DATEADD(" + expected + ",10,[test_column])"));
    }

    private SqlFunctionScalar createSqlFunctionScalarForDateTest(final ScalarFunction scalarFunction) {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlColumn(1,
                ColumnMetadata.builder().name("test_column")
                        .adapterNotes("{\"jdbcDataType\":93, " + "\"typeName\":\"TIMESTAMP\"}")
                        .type(DataType.createChar(20, DataType.ExaCharset.UTF8)).build()));
        arguments.add(new SqlLiteralExactnumeric(new BigDecimal(10)));
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
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarForDateTest(scalarFunction);
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo("DATEDIFF(" + expected + ",10,[test_column])"));
    }

    @CsvSource({ "CURRENT_DATE, CAST(GETDATE() AS DATE)", //
            "CURRENT_TIMESTAMP,  GETDATE()", //
            "SYSDATE, CAST( SYSDATETIME() AS DATE)", //
            "SYSTIMESTAMP, SYSDATETIME()" })
    @ParameterizedTest
    void testVisitSqlFunctionScalarWithoutArguments(final ScalarFunction scalarFunction, final String expected)
            throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(scalarFunction, null);
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo(expected));
    }

    @CsvSource(value = { "INSTR : CHARINDEX('test2', 'test', 'test3')", //
            "LPAD : RIGHT ( REPLICATE('test3','test2') + LEFT('test','test2'),'test2')", //
            "RPAD : LEFT(RIGHT('test','test2') + REPLICATE('test3','test2'),'test2')" //
    }, delimiter = ':')
    @ParameterizedTest
    void testVisitSqlFunctionScalarWithThreeArguments(final ScalarFunction scalarFunction, final String expected)
            throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralString("test"));
        arguments.add(new SqlLiteralString("test2"));
        arguments.add(new SqlLiteralString("test3"));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(scalarFunction, arguments);
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo(expected));
    }

    @CsvSource(value = { "ST_X : 'left'.STX", //
            "ST_Y : 'left'.STY", //
            "ST_ENDPOINT : CAST('left'.STEndPoint()as VARCHAR(8000) )", //
            "ST_ISCLOSED : 'left'.STIsClosed()", //
            "ST_ISRING : 'left'.STIsRing()", //
            "ST_LENGTH : 'left'.STLength()", //
            "ST_NUMPOINTS : 'left'.STNumPoints()", //
            "ST_POINTN : CAST('left'.STPointN('right')as VARCHAR(8000) )", //
            "ST_STARTPOINT : CAST('left'.STStartPoint()as VARCHAR(8000) )", //
            "ST_AREA : 'left'.STArea()", //
            "ST_EXTERIORRING : CAST('left'.STExteriorRing()as VARCHAR(8000) )", //
            "ST_INTERIORRINGN : CAST('left'.STInteriorRingN ('right')as VARCHAR(8000) )", //
            "ST_NUMINTERIORRINGS : 'left'.STNumInteriorRing()", //
            "ST_GEOMETRYN : CAST('left'.STGeometryN('right')as VARCHAR(8000) )", //
            "ST_NUMGEOMETRIES : 'left'.STNumGeometries()", //
            "ST_BOUNDARY : CAST('left'.STBoundary()as VARCHAR(8000) )", //
            "ST_BUFFER : CAST('left'.STBuffer('right')as VARCHAR(8000) )", //
            "ST_CENTROID : CAST('left'.STCentroid()as VARCHAR(8000) )", //
            "ST_CONTAINS : 'left'.STContains('right')", //
            "ST_CONVEXHULL : CAST('left'.STConvexHull()as VARCHAR(8000) )", //
            "ST_CROSSES : 'left'.STCrosses('right')", //
            "ST_DIFFERENCE : CAST('left'.STDifference('right')as VARCHAR(8000) )", //
            "ST_DIMENSION : 'left'.STDimension()", //
            "ST_DISJOINT : CAST('left'.STDisjoint('right')as VARCHAR(8000) )", //
            "ST_DISTANCE : 'left'.STDistance('right')", //
            "ST_ENVELOPE : CAST('left'.STEnvelope()as VARCHAR(8000) )", //
            "ST_EQUALS : 'left'.STEquals('right')", //
            "ST_GEOMETRYTYPE : 'left'.STGeometryType()", //
            "ST_INTERSECTION : CAST('left'.STIntersection('right')as VARCHAR(8000) )", //
            "ST_INTERSECTS : 'left'.STIntersects('right')", //
            "ST_ISEMPTY : 'left'.STIsEmpty()", //
            "ST_ISSIMPLE : 'left'.STIsSimple()", //
            "ST_OVERLAPS : 'left'.STOverlaps('right')", //
            "ST_SYMDIFFERENCE : CAST('left'.STSymDifference ('right')as VARCHAR(8000) )", //
            "ST_TOUCHES : 'left'.STTouches('right')", //
            "ST_UNION : CAST('left'.STUnion('right')as VARCHAR(8000) )", //
            "ST_WITHIN : 'left'.STWithin('right')", //
            "BIT_AND : 'left' & '" + "" + "right'", //
            "BIT_OR : 'left' | 'right'", //
            "BIT_XOR : 'left' ^ 'right'", //
            "BIT_NOT : ~ 'left'", //
            "HASH_MD5 : CONVERT(Char, HASHBYTES('MD5','left'), 2)", //
            "HASH_SHA1 : CONVERT(Char, HASHBYTES('SHA1','left'), 2)", //
            "ZEROIFNULL : ISNULL('left',0)" //
    }, delimiter = ':')
    @ParameterizedTest
    void testVisitSqlFunctionScalarWithTwoArguments(final ScalarFunction scalarFunction, final String expected)
            throws AdapterException {
        final List<SqlNode> arguments = List.of(new SqlLiteralString("left"), new SqlLiteralString("right"));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(scalarFunction, arguments);
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo(expected));
    }
}