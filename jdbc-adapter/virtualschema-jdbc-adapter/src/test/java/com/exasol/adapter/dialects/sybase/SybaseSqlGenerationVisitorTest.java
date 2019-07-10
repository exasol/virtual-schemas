package com.exasol.adapter.dialects.sybase;

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
import static utils.SqlNodesCreator.*;

class SybaseSqlGenerationVisitorTest {
    private SybaseSqlGenerationVisitor visitor;
    @Mock
    private Connection connectionMock;

    @BeforeEach
    void beforeEach() {
        final SqlDialect dialect = new SybaseSqlDialect(this.connectionMock, AdapterProperties.emptyProperties());
        final SqlGenerationContext context = new SqlGenerationContext("test_catalog", "test_schema", false);
        this.visitor = new SybaseSqlGenerationVisitor(dialect, context);
    }

    @Test
    void testVisitSqlSelectListAnyValue() throws AdapterException {
        final SqlSelectList sqlSelectList = SqlSelectList.createAnyValueSelectList();
        assertThat(visitor.visit(sqlSelectList), equalTo("true"));
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
        final SqlSelectList sqlSelectList = SqlSelectList.createSelectStarSelectList();
        final List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(ColumnMetadata.builder().name("test_column")
                .adapterNotes("{\"jdbcDataType\":2009, \"typeName\":\"" + typeName + "\"}")
                .type(DataType.createVarChar(10, DataType.ExaCharset.UTF8)).build());
        final SqlNode sqlStatementSelect = createSqlStatementSelect(sqlSelectList, columns, "test_table");
        sqlSelectList.setParent(sqlStatementSelect);
        assertThat(visitor.visit(sqlSelectList), equalTo(expected));
    }

    @CsvSource({ "varbinary", //
            "binary", //
            "image" //
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
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("DATEADD(" + expected + ",10,[test_column])"));
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
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("DATEDIFF(" + expected + ",10,[test_column])"));
    }

    @CsvSource({ "CURRENT_DATE, CAST( GETDATE() AS DATE)", //
          "CURRENT_TIMESTAMP,  GETDATE()", //
          "SYSDATE, CAST( SYSDATETIME() AS DATE)", //
          "SYSTIMESTAMP, SYSDATETIME()" })
    @ParameterizedTest
    void testVisitSqlFunctionScalarWithoutArguments(final ScalarFunction scalarFunction, final String expected)
          throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(scalarFunction, null, true, false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo(expected));
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
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(scalarFunction, arguments, true, false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo(expected));
    }

    @CsvSource(value = { "ST_X : 'test'.STX", //
          "ST_Y : 'test'.STY", //
          "ST_ENDPOINT : CAST('test'.STEndPoint()as VARCHAR(8000) )", //
          "ST_ISCLOSED : 'test'.STIsClosed()", //
          "ST_ISRING : 'test'.STIsRing()", //
          "ST_LENGTH : 'test'.STLength()", //
          "ST_NUMPOINTS : 'test'.STNumPoints()", //
          "ST_POINTN : CAST('test'.STPointN('test2')as VARCHAR(8000) )", //
          "ST_STARTPOINT : CAST('test'.STStartPoint()as VARCHAR(8000) )", //
          "ST_AREA : 'test'.STArea()", //
          "ST_EXTERIORRING : CAST('test'.STExteriorRing()as VARCHAR(8000) )", //
          "ST_INTERIORRINGN : CAST('test'.STInteriorRingN ('test2')as VARCHAR(8000) )", //
          "ST_NUMINTERIORRINGS : 'test'.STNumInteriorRing()", //
          "ST_GEOMETRYN : CAST('test'.STGeometryN('test2')as VARCHAR(8000) )", //
          "ST_NUMGEOMETRIES : 'test'.STNumGeometries()", //
          "ST_BOUNDARY : CAST('test'.STBoundary()as VARCHAR(8000) )", //
          "ST_BUFFER : CAST('test'.STBuffer('test2')as VARCHAR(8000) )", //
          "ST_CENTROID : CAST('test'.STCentroid()as VARCHAR(8000) )", //
          "ST_CONTAINS : 'test'.STContains('test2')", //
          "ST_CONVEXHULL : CAST('test'.STConvexHull()as VARCHAR(8000) )", //
          "ST_CROSSES : 'test'.STCrosses('test2')", //
          "ST_DIFFERENCE : CAST('test'.STDifference('test2')as VARCHAR(8000) )", //
          "ST_DIMENSION : 'test'.STDimension()", //
          "ST_DISJOINT : CAST('test'.STDisjoint('test2')as VARCHAR(8000) )", //
          "ST_DISTANCE : 'test'.STDistance('test2')", //
          "ST_ENVELOPE : CAST('test'.STEnvelope()as VARCHAR(8000) )", //
          "ST_EQUALS : 'test'.STEquals('test2')", //
          "ST_GEOMETRYTYPE : 'test'.STGeometryType()", //
          "ST_INTERSECTION : CAST('test'.STIntersection('test2')as VARCHAR(8000) )", //
          "ST_INTERSECTS : 'test'.STIntersects('test2')", //
          "ST_ISEMPTY : 'test'.STIsEmpty()", //
          "ST_ISSIMPLE : 'test'.STIsSimple()", //
          "ST_OVERLAPS : 'test'.STOverlaps('test2')", //
          "ST_SYMDIFFERENCE : CAST('test'.STSymDifference ('test2')as VARCHAR(8000) )", //
          "ST_TOUCHES : 'test'.STTouches('test2')", //
          "ST_UNION : CAST('test'.STUnion('test2')as VARCHAR(8000) )", //
          "ST_WITHIN : 'test'.STWithin('test2')", //
          "BIT_AND : 'test' & 'test2'", //
          "BIT_OR : 'test' | 'test2'", //
          "BIT_XOR : 'test' ^ 'test2'", //
          "BIT_NOT : ~ 'test'", //
          "HASH_MD5 : CONVERT(Char, HASHBYTES('MD5','test'), 2)", //
          "HASH_SHA1 : CONVERT(Char, HASHBYTES('SHA1','test'), 2)", //
          "HASH_SHA : CONVERT(Char, HASHBYTES('SHA','test'), 2)", //
          "ZEROIFNULL : ISNULL('test',0)" //
    }, delimiter = ':')
    @ParameterizedTest
    void testVisitSqlFunctionScalarWithTwoArguments(final ScalarFunction scalarFunction, final String expected)
          throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarWithTwoStringArguments(scalarFunction,
              "test", "test2");
        assertThat(visitor.visit(sqlFunctionScalar), equalTo(expected));
    }
}