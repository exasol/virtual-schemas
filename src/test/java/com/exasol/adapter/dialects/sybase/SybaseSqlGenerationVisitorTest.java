package com.exasol.adapter.dialects.sybase;

import static com.exasol.adapter.dialects.VisitorAssertions.assertSqlNodeConvertedToAsterisk;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static utils.SqlNodesCreator.*;

import java.util.ArrayList;
import java.util.List;

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
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.sql.*;

@ExtendWith(MockitoExtension.class)
class SybaseSqlGenerationVisitorTest {
    private SybaseSqlGenerationVisitor visitor;

    @BeforeEach
    void beforeEach(@Mock final ConnectionFactory connectionFactoryMock) {
        final SqlDialect dialect = new SybaseSqlDialect(connectionFactoryMock, AdapterProperties.emptyProperties());
        final SqlGenerationContext context = new SqlGenerationContext("test_catalog", "test_schema", false);
        this.visitor = new SybaseSqlGenerationVisitor(dialect, context);
    }

    @Test
    void testVisitSqlSelectListAnyValue() throws AdapterException {
        final SqlSelectList sqlSelectList = SqlSelectList.createAnyValueSelectList();
        assertThat(this.visitor.visit(sqlSelectList), equalTo("true"));
    }

    @Test
    void testVisitSqlSelectListSelectStar() throws AdapterException {
        final SqlSelectList sqlSelectList = createSqlSelectStarListWithoutColumns();
        assertSqlNodeConvertedToAsterisk(sqlSelectList, this.visitor);
    }

    @Test
    void testVisitSqlStatementSelect() throws AdapterException {
        final SqlStatementSelect select = (SqlStatementSelect) DialectTestData.getTestSqlNode();
        assertThat(this.visitor.visit(select), //
                equalTo("SELECT TOP 10 [USER_ID], COUNT([URL])" //
                        + " FROM [test_catalog].[test_schema].[CLICKS]" //
                        + " WHERE 1 < [USER_ID]" //
                        + " GROUP BY [USER_ID] HAVING 1 < COUNT([URL])" //
                        + " ORDER BY (CASE WHEN [USER_ID] IS NULL THEN 1 ELSE 0 END), [USER_ID]"));
    }

    @CsvSource(value = { "text : CAST([test_column]  as NVARCHAR(4000) )", //
            "time : CONVERT(VARCHAR(12), [test_column], 137)", //
            "bigtime : CONVERT(VARCHAR(16), [test_column], 137)", //
            "xml : CAST([test_column]  as NVARCHAR(4000) )" //
    }, delimiter = ':')
    @ParameterizedTest
    void testVisitSqlSelectListSelectStarRequiresCast(final String typeName, final String expected)
            throws AdapterException {
        final SqlSelectList sqlSelectList = createSqlSelectStarListWithOneColumn(
                "{\"jdbcDataType\":2009, \"typeName\":\"" + typeName + "\"}",
                DataType.createVarChar(10, DataType.ExaCharset.UTF8), "test_column");
        assertThat(this.visitor.visit(sqlSelectList), equalTo(expected));
    }

    @CsvSource({ "varbinary", //
            "binary", //
            "image" //
    })
    @ParameterizedTest
    void testVisitSqlSelectListSelectStarUnsupportedType(final String typeName) throws AdapterException {
        final SqlSelectList sqlSelectList = createSqlSelectStarListWithOneColumn(
                "{\"jdbcDataType\":2009, \"typeName\":\"" + typeName + "\"}",
                DataType.createVarChar(10, DataType.ExaCharset.UTF8), "test_column");
        SqlSelectList.createSelectStarSelectList();
        assertThat(this.visitor.visit(sqlSelectList), equalTo("'" + typeName + " NOT SUPPORTED'"));
    }

    @Test
    void testVisitSqlSelectListSelectStarThrowsException() {
        final SqlSelectList sqlSelectList = createSqlSelectStarListWithOneColumn("",
                DataType.createVarChar(10, DataType.ExaCharset.UTF8), "test_column");
        SqlSelectList.createSelectStarSelectList();
        assertThrows(SqlGenerationVisitorException.class, () -> this.visitor.visit(sqlSelectList));
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
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarForDateTest(scalarFunction, 10);
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo("DATEADD(" + expected + ",10,[test_column])"));
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
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo("DATEDIFF(" + expected + ",10,[test_column])"));
    }

    @CsvSource({ "CURRENT_DATE, CAST( GETDATE() AS DATE)", //
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
            "BIT_AND : 'left' & 'right'", //
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
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarWithTwoStringArguments(scalarFunction,
                "left", "right");
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo(expected));
    }
}