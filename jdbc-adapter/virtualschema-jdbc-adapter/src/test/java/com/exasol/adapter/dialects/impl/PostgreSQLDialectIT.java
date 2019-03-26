package com.exasol.adapter.dialects.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import com.exasol.adapter.AdapterException;
import com.exasol.jdbc.DataException;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import com.exasol.adapter.dialects.AbstractIntegrationTest;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Integration test for the PostgreSQL SQL dialect
 *
 */
public class PostgreSQLDialectIT extends AbstractIntegrationTest {
    private static final String VIRTUAL_SCHEMA = "PG";
    private static final String VIRTUAL_SCHEMA_UPPERCASE_TABLE = "PG_UPPER";
    private static final String VIRTUAL_SCHEMA_PRESERVE_ORIGINAL_CASE = "PG_PRESERVE_CASE";
    private static final String POSTGRES_CATALOG = "postgres";
    private static final String POSTGRES_SCHEMA = "public";
    private static final String POSTGRES_SCHEMA_UPPERCASE_TABLE = "upper";
    private static final String POSTGRES_TABLE_DATATYPES = "all_datatypes";
    private static final boolean IS_LOCAL = false;

    private Map<String, String> columnTypes;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @BeforeClass
    public static void setUpClass() throws FileNotFoundException, SQLException, ClassNotFoundException {
        Assume.assumeTrue(getConfig().getPostgresqlTestsRequested());
        setConnection(connectToExa());

        createTestSchema();
        createTestSchemaUpperCaseTable();
        createPostgreSQLJDBCAdapter();
        createVirtualSchema(VIRTUAL_SCHEMA, PostgreSQLSqlDialect.getPublicName(), POSTGRES_CATALOG, POSTGRES_SCHEMA, "", getConfig().getPostgresqlUser(),
                getConfig().getPostgresqlPassword(), "ADAPTER.JDBC_ADAPTER", getConfig().getPostgresqlDockerJdbcConnectionString(),
                IS_LOCAL, getConfig().debugAddress(), "", null, "");
        createVirtualSchema(VIRTUAL_SCHEMA_UPPERCASE_TABLE, PostgreSQLSqlDialect.getPublicName(), POSTGRES_CATALOG, POSTGRES_SCHEMA_UPPERCASE_TABLE, "",
                getConfig().getPostgresqlUser(), getConfig().getPostgresqlPassword(), "ADAPTER.JDBC_ADAPTER",
                getConfig().getPostgresqlDockerJdbcConnectionString(), IS_LOCAL, getConfig().debugAddress(), "",
                "ignore_errors='POSTGRESQL_UPPERCASE_TABLES'", "");
        createVirtualSchema(VIRTUAL_SCHEMA_PRESERVE_ORIGINAL_CASE, PostgreSQLSqlDialect.getPublicName(), POSTGRES_CATALOG, POSTGRES_SCHEMA_UPPERCASE_TABLE, "",
                getConfig().getPostgresqlUser(), getConfig().getPostgresqlPassword(), "ADAPTER.JDBC_ADAPTER",
                getConfig().getPostgresqlDockerJdbcConnectionString(), IS_LOCAL, getConfig().debugAddress(), "",
                "POSTGRESQL_IDENTIFIER_MAPPING = 'PRESERVE_ORIGINAL_CASE'", "");
    }

    private static void createTestSchema() throws SQLException, ClassNotFoundException, FileNotFoundException {
        final String user = getConfig().getPostgresqlUser();
        final String password = getConfig().getPostgresqlPassword();
        final String postgresConnectionString = getConfig().getPostgresqlJdbcConnectionString();
        Class.forName("org.postgresql.Driver");
        try (final Connection conn = DriverManager.getConnection(postgresConnectionString, user, password))
        {
            final Statement stmt = conn.createStatement();
            stmt.execute(String.format("create table %s.t(x int)", POSTGRES_SCHEMA));
            stmt.execute(String.format("insert into %s.t values (1);", POSTGRES_SCHEMA));
            stmt.execute(String.format(
                "create table %s.%s (" +
                    "mybigint bigint,	" +
                    "mybigserial bigserial,	" +
                    "mybit bit, 	" +
                    "mybitVar bit varying  (5) ," +
                    "myBoolean boolean,		" +
                    "myBox box,	 	" +
                    "myBytea bytea,	 	" +
                    "myCharacter character (1000)," +
                    "myCharacterVar character varying  (1000)," +
                    "myCidr cidr	, 	" +
                    "myCircle circle	 ," +
                    "myDate date	, 	" +
                    "myDouble double precision," +
                    "myinet inet	, 	" +
                    "myInteger integer," +
                    "myInterval interval," +
                    "myJson json, 	" +
                    "myJsonB jsonb,	 	" +
                    "myLine line,	 	" +
                    "myLseg lseg,	 	" +
                    "myMacAddr macaddr,	 	" +
                    "myMoney money,	 	" +
                    "myNumeric numeric(36, 10)," +
                    "myPath path,	 	" +
                    "mypoint point,	 	" +
                    "mypolygon polygon,	 	" +
                    "myreal real	,     " +
                    "mysmallint smallint," +
                    "mytext text,	 	" +
                    "mytime time," +
                    "mytimeWithTimeZone time with time zone," +
                    "mytimestamp timestamp, 	 	" +
                    "mytimestampWithTimeZone timestamp with time zone," +
                    "mytsquery tsquery,	 	" +
                    "mytsvector tsvector	, 	" +
                    "myuuid uuid,	 	" +
                    "myxml xml	 	" +
                ")"
            , POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mybigint) VALUES(10000000000)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mybigserial) VALUES(nextval('all_datatypes_mybigserial_seq'::regclass))", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mybit) VALUES(B'1')", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mybitvar) VALUES(B'0')", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(myboolean) VALUES(false)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mybox) VALUES('( ( 1 , 8 ) , ( 4 , 16 ) )')", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mybytea) VALUES(E'\\\\000'::bytea)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mycharacter) VALUES('hajksdf')", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mycharactervar) VALUES('hjkdhjgfh')", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mycidr) VALUES('192.168.100.128/25'::cidr)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mycircle) VALUES('( ( 1 , 5 ) , 3 )'::circle)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mydate) VALUES('2010-01-01')", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mydouble) VALUES(192189234.1723854)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(myinet) VALUES('192.168.100.128'::inet)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(myinteger) VALUES(7189234)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(myinterval) VALUES(INTERVAL '1' YEAR)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(myjson) VALUES('{\"bar\": \"baz\", \"balance\": 7.77, \"active\": false}'::json)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(myjsonb) VALUES('{\"bar\": \"baz\", \"balance\": 7.77, \"active\": false}'::jsonb)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(myline) VALUES('{ 1, 2, 3 }'::line)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mylseg) VALUES('[ ( 1 , 2 ) , ( 3 , 4 ) ]'::lseg)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mymacaddr) VALUES('08:00:2b:01:02:03'::macaddr)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mymoney) VALUES(100.01)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mynumeric) VALUES(24.23)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mypath) VALUES('[ ( 1 , 2 ) , ( 3 , 4 ) ]'::path)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mypoint) VALUES('( 1 , 3 )'::point)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mypolygon) VALUES('( ( 1 , 2 ) , (2,4),(3,7) )'::polygon)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(myreal) VALUES(10.12)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mysmallint) VALUES(100)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mytext) VALUES('skldfjgkl jsdklfgjklsdjfg jsklfdjg')", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mytime) VALUES('11:11:11')", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mytimeWithTimeZone) VALUES('11:11:11 +01:00')", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mytimestamp) VALUES('2010-01-01 11:11:11')", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mytimestampwithtimezone) VALUES('2010-01-01 11:11:11 +01:00')", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mytsquery) VALUES('fat & rat'::tsquery)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mytsvector) VALUES(to_tsvector('english', 'The Fat Rats'))", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(myuuid) VALUES('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(myxml) VALUES(XMLPARSE (DOCUMENT '<?xml version=\"1.0\"?><book><title>Manual</title><chapter>...</chapter></book>'))", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));

            stmt.execute(String.format("create table %s.t1(x int, y varchar(100))", POSTGRES_SCHEMA));
            stmt.execute(String.format("insert into %s.t1 values (1,'aaa'), (2,'bbb');", POSTGRES_SCHEMA));
            stmt.execute(String.format("create table %s.t2(x int, y varchar(100))", POSTGRES_SCHEMA));
            stmt.execute(String.format("insert into %s.t2 values (2,'bbb'), (3,'ccc');", POSTGRES_SCHEMA));
        }
    }

    private static void createTestSchemaUpperCaseTable() throws SQLException, ClassNotFoundException, FileNotFoundException {
        final String user = getConfig().getPostgresqlUser();
        final String password = getConfig().getPostgresqlPassword();
        final String postgresConnectionString = getConfig().getPostgresqlJdbcConnectionString();
        Class.forName("org.postgresql.Driver");
        try (final Connection conn = DriverManager.getConnection(postgresConnectionString, user, password))
        {
            final Statement stmt = conn.createStatement();
            stmt.execute(String.format("create schema %s", POSTGRES_SCHEMA_UPPERCASE_TABLE));
            stmt.execute(String.format("create table %s.lower_t(x int, y int)", POSTGRES_SCHEMA_UPPERCASE_TABLE));
            stmt.execute(String.format("create table %s.\"UPPer_t\"(x int, \"Y\" int)", POSTGRES_SCHEMA_UPPERCASE_TABLE));
        }
    }

    @Test
    public void testOpenVirtualSchema() throws SQLException {
        final String open_schema_query = String.format("OPEN SCHEMA %s", VIRTUAL_SCHEMA);
        execute(open_schema_query);
    }

    @Test
    public void testSelectSingleColumn() throws SQLException {
        final String query = String.format("SELECT x FROM %s.t", VIRTUAL_SCHEMA);
        final ResultSet result = executeQuery(query);
        matchNextRow(result, 1L);
    }

    // Join Tests -------------------------------------------------------------
    @Test
    public void innerJoin() throws SQLException {
        final String query = String.format("SELECT * FROM  %1$s.t1 a INNER JOIN  %1$s.t2 b ON a.x=b.x", VIRTUAL_SCHEMA);
        final ResultSet result = executeQuery(query);
        matchNextRow(result, (long) 2, "bbb", (long) 2 ,"bbb");
        assertFalse(result.next());
    }

    @Test
    public void innerJoinWithProjection() throws SQLException {
        final String query = String.format("SELECT b.y || %1$s.t1.y FROM  %1$s.t1 INNER JOIN  %1$s.t2 b ON %1$s.t1.x=b.x", VIRTUAL_SCHEMA);
        final ResultSet result = executeQuery(query);
        matchNextRow(result, "bbbbbb");
        assertFalse(result.next());
    }

    @Test
    public void leftJoin() throws SQLException {
        final String query = String.format("SELECT * FROM  %1$s.t1 a LEFT OUTER JOIN  %1$s.t2 b ON a.x=b.x ORDER BY a.x", VIRTUAL_SCHEMA);
        final ResultSet result = executeQuery(query);
        matchNextRow(result, (long) 1, "aaa", null ,null);
        matchNextRow(result, (long) 2, "bbb", (long) 2 ,"bbb");
        assertFalse(result.next());
    }

    @Test
    public void rightJoin() throws SQLException {
        final String query = String.format("SELECT * FROM  %1$s.t1 a RIGHT OUTER JOIN  %1$s.t2 b ON a.x=b.x ORDER BY a.x", VIRTUAL_SCHEMA);
        final ResultSet result = executeQuery(query);
        matchNextRow(result, (long) 2, "bbb", (long) 2 ,"bbb");
        matchNextRow(result, null, null, (long) 3 ,"ccc");
        assertFalse(result.next());
    }

    @Test
    public void fullOuterJoin() throws SQLException {
        final String query = String.format("SELECT * FROM  %1$s.t1 a FULL OUTER JOIN  %1$s.t2 b ON a.x=b.x ORDER BY a.x", VIRTUAL_SCHEMA);
        final ResultSet result = executeQuery(query);
        matchNextRow(result, (long) 1, "aaa", null ,null);
        matchNextRow(result, (long) 2, "bbb", (long) 2 ,"bbb");
        matchNextRow(result, null, null, (long) 3 ,"ccc");
        assertFalse(result.next());
    }

    // Identifier Test - CONVERT_TO_UPPER mode --------------------------------
    @Test
    public void testCreateSchemaWithUpperCaseTables() throws SQLException, FileNotFoundException {
        this.expectedEx.expect(DataException.class);
        this.expectedEx.expectMessage("Table UPPer_t cannot be used in virtual schema. Set property IGNORE_ERRORS to POSTGRESQL_UPPERCASE_TABLES to enforce schema creation.");
        createVirtualSchema("FOO", PostgreSQLSqlDialect.getPublicName(), POSTGRES_CATALOG, POSTGRES_SCHEMA_UPPERCASE_TABLE, "",
                getConfig().getPostgresqlUser(), getConfig().getPostgresqlPassword(), "ADAPTER.JDBC_ADAPTER",
                getConfig().getPostgresqlDockerJdbcConnectionString(), IS_LOCAL, getConfig().debugAddress(), "", null, "JOIN");
    }

    @Test
    public void testQueryUpperCaseTableQuoted() throws SQLException {
        this.expectedEx.expect(DataException.class);
        this.expectedEx.expectMessage("Cannot resolve column types");
        final String query = String.format("SELECT x FROM  %s.\"UPPer_t\"", VIRTUAL_SCHEMA_UPPERCASE_TABLE);
        final ResultSet result = executeQuery(query);
    }

    @Test
    public void testQueryUpperCaseTable() throws SQLException {
        this.expectedEx.expect(SQLException.class);
        this.expectedEx.expectMessage("object PG_UPPER.UPPER_T not found");
        final String query = String.format("SELECT x FROM  %s.UPPer_t", VIRTUAL_SCHEMA_UPPERCASE_TABLE);
        final ResultSet result = executeQuery(query);
    }

    @Test
    public void testQueryLowerCaseTable() throws SQLException {
        final String query = String.format("SELECT x FROM  %s.lower_t", VIRTUAL_SCHEMA_UPPERCASE_TABLE);
        final ResultSet result = executeQuery(query);
        assertFalse(result.next());
    }

    @Test
    public void testUnsetIgnoreUpperCaseTablesAndRefresh() throws SQLException {
        this.expectedEx.expect(DataException.class);
        this.expectedEx.expectMessage("Table UPPer_t cannot be used in virtual schema. Set property IGNORE_ERRORS to POSTGRESQL_UPPERCASE_TABLES to enforce schema creation.");
        final String alter_schema_query = String.format("ALTER VIRTUAL SCHEMA %s set ignore_errors=''", VIRTUAL_SCHEMA_UPPERCASE_TABLE);
        execute(alter_schema_query);
        final String alter_schema_query_identifier_mapping = String.format("ALTER VIRTUAL SCHEMA %s set POSTGRESQL_IDENTIFIER_MAPPING = 'CONVERT_TO_UPPER'", VIRTUAL_SCHEMA_UPPERCASE_TABLE);
        execute(alter_schema_query_identifier_mapping);
        final String refresh_schema_query = String.format("ALTER VIRTUAL SCHEMA %s REFRESH", VIRTUAL_SCHEMA_UPPERCASE_TABLE);
        execute(refresh_schema_query);
    }

    @Test
    public void testSetIgnoreUpperCaseTablesAndRefresh() throws SQLException {
        final String alter_schema_query = String.format("ALTER VIRTUAL SCHEMA %s set ignore_errors='POSTGRESQL_UPPERCASE_TABLES'", VIRTUAL_SCHEMA_UPPERCASE_TABLE);
        execute(alter_schema_query);
        final String refresh_schema_query = String.format("ALTER VIRTUAL SCHEMA %s REFRESH", VIRTUAL_SCHEMA_UPPERCASE_TABLE);
        execute(refresh_schema_query);
    }

    // Identifier Test - PRESERVE_ORIGINAL_CASE mode --------------------------------
    @Test
    public void testPreserveCaseQueryLowerCaseTableFails() throws SQLException {
        this.expectedEx.expect(SQLException.class);
        this.expectedEx.expectMessage("object PG_PRESERVE_CASE.LOWER_T not found");
        final String query = String.format("SELECT x FROM  %s.lower_t", VIRTUAL_SCHEMA_PRESERVE_ORIGINAL_CASE);
        final ResultSet result = executeQuery(query);
    }

    @Test
    public void testPreserveCaseQueryLowerCaseTableWithQuotes() throws SQLException {
        final String query = String.format("SELECT \"x\" FROM  %s.\"lower_t\"", VIRTUAL_SCHEMA_PRESERVE_ORIGINAL_CASE);
        final ResultSet result = executeQuery(query);
        assertFalse(result.next());
    }

    @Test
    public void testPreserveCaseQueryUpperCaseTableWithQuotes() throws SQLException {
        final String query = String.format("SELECT \"Y\" FROM  %s.\"UPPer_t\"", VIRTUAL_SCHEMA_PRESERVE_ORIGINAL_CASE);
        final ResultSet result = executeQuery(query);
        assertFalse(result.next());
    }

    @Test
    public void testPreserveCaseQueryUpperCaseTableWithQuotesLowerCaseColumn() throws SQLException {
        final String query = String.format("SELECT \"x\" FROM  %s.\"UPPer_t\"", VIRTUAL_SCHEMA_PRESERVE_ORIGINAL_CASE);
        final ResultSet result = executeQuery(query);
        assertFalse(result.next());
    }

    // Datatype tests ---------------------------------------------------------
    @Test
    public void testDatatypeBigint() throws SQLException {
        testDatatype("mybigint", new BigDecimal("10000000000"), "DECIMAL(19,0)");
    }
    @Test
    public void testDatatypeBigSerial() throws SQLException {
        testDatatype("mybigserial", new BigDecimal("1"), "DECIMAL(19,0)");
    }
    @Test
    public void testDatatypeBit() throws SQLException {
        testDatatype("mybit",  Boolean.TRUE, "BOOLEAN");
    }
    @Test
    public void testDatatypeBitVar() throws SQLException {
        testDatatype("mybitvar",  Boolean.FALSE, "VARCHAR(5) UTF8");
    }
    @Test
    public void testDatatypeBoolean() throws SQLException {
        testDatatype("myboolean",  Boolean.FALSE, "BOOLEAN");
    }
    @Test
    public void testDatatypeBox() throws SQLException {
        testDatatype("mybox", "(4,16),(1,8)", "VARCHAR(2000000) UTF8");
    }
    @Test
    public void testDatatypeBytea() throws SQLException {
        testDatatype("mybytea", "bytea NOT SUPPORTED", "VARCHAR(2000000) UTF8");
    }
    @Test
    public void testDatatypeCharacter() throws SQLException {
        final String empty = " ";
        final String expected = "hajksdf" + String.join("", Collections.nCopies(993, empty));
        testDatatype("mycharacter", expected, "CHAR(1000) ASCII");
    }
    @Test
    public void testDatatypeCharacterVar() throws SQLException {
        testDatatype("mycharactervar", "hjkdhjgfh", "VARCHAR(1000) ASCII");
    }
    @Test
    public void testDatatypeCidr() throws SQLException {
        testDatatype("mycidr", "192.168.100.128/25", "VARCHAR(2000000) UTF8");
    }
    @Test
    public void testDatatypeCircle() throws SQLException {
        testDatatype("mycircle", "<(1,5),3>", "VARCHAR(2000000) UTF8");
    }
    @Test
    public void testDatatypeDate() throws SQLException {
        testDatatype("mydate", "2010-01-01", "DATE");
    }
    @Test
    public void testDatatypeDouble() throws SQLException {
        testDatatype("mydouble", new Double("192189234.1723854"), "DOUBLE");
    }
    @Test
    public void testDatatypeInet() throws SQLException {
        testDatatype("myinet", "192.168.100.128/32", "VARCHAR(2000000) UTF8");
    }
    @Test
    public void testDatatypeInteger() throws SQLException {
        testDatatype("myinteger", new Integer("7189234"), "DECIMAL(10,0)");
    }
    @Test
    public void testDatatypeIntervalYM() throws SQLException {
        testDatatype("myinterval", "1 year", "VARCHAR(2000000) UTF8");
    }
    @Test
    public void testDatatypeJSON() throws SQLException {
        testDatatype("myjson", "{\"bar\": \"baz\", \"balance\": 7.77, \"active\": false}", "VARCHAR(2000000) UTF8");
    }
    @Test
    public void testDatatypeJSONB() throws SQLException {
        testDatatype("myjsonb", "{\"bar\": \"baz\", \"active\": false, \"balance\": 7.77}", "VARCHAR(2000000) UTF8");
    }
    @Test
    public void testDatatypeLine() throws SQLException {
        testDatatype("myline", "{1,2,3}", "VARCHAR(2000000) UTF8");
    }
    @Test
    public void testDatatypeLSeg() throws SQLException {
        testDatatype("mylseg", "[(1,2),(3,4)]", "VARCHAR(2000000) UTF8");
    }
    @Test
    public void testDatatypeMACAddr() throws SQLException {
        testDatatype("mymacaddr", "08:00:2b:01:02:03", "VARCHAR(2000000) UTF8");
    }
    @Test
    public void testDatatypeMoney() throws SQLException {
        testDatatype("mymoney", "100.01", "DOUBLE");
    }
    @Test
    public void testDatatypeNumeric() throws SQLException {
        testDatatype("mynumeric", new BigDecimal("24.2300000000"), "VARCHAR(2000000) UTF8");
    }
    @Test
    public void testDatatypePath() throws SQLException {
        testDatatype("mypath", "[(1,2),(3,4)]", "VARCHAR(2000000) UTF8");
    }
    @Test
    public void testDatatypePoint() throws SQLException {
        testDatatype("mypoint", "(1,3)", "VARCHAR(2000000) UTF8");
    }
    @Test
    public void testDatatypePolygon() throws SQLException {
        testDatatype("mypolygon", "((1,2),(2,4),(3,7))", "VARCHAR(2000000) UTF8");
    }
    @Test
    public void testDatatypeReal() throws SQLException {
        testDatatype("myreal", new Float("10.12"), "DOUBLE");
    }
    @Test
    public void testDatatypeSmallInt() throws SQLException {
        testDatatype("mysmallint", new Integer("100"), "DECIMAL(5,0)");
    }
    @Test
    public void testDatatypeText() throws SQLException {
        testDatatype("mytext", "skldfjgkl jsdklfgjklsdjfg jsklfdjg", "VARCHAR(2000000) ASCII");
    }
    @Test
    public void testDatatypeTime() throws SQLException {
        testDatatype("mytime", "1970-01-01 11:11:11.0", "VARCHAR(2000000) UTF8");
    }
    @Test
    public void testDatatypeTimeWithTimezone() throws SQLException {
        testDatatype("mytimeWithTimeZone", "1970-01-01 10:11:11.0", "VARCHAR(2000000) UTF8");
    }
    @Test
    public void testDatatypeTimestamp() throws SQLException {
        testDatatype("mytimestamp", "2010-01-01 11:11:11.000000", "TIMESTAMP");
    }
    @Test
    public void testDatatypeTimestampWithTimezone() throws SQLException {
        testDatatype("mytimestampwithtimezone", "2010-01-01 10:11:11.000000", "TIMESTAMP");
    }
    @Test
    public void testDatatypeTsQuery() throws SQLException {
        testDatatype("mytsquery", "'fat' & 'rat'", "VARCHAR(2000000) UTF8");
    }
    @Test
    public void testDatatypeTsvector() throws SQLException {
        testDatatype("mytsvector", "'fat':2 'rat':3", "VARCHAR(2000000) UTF8");
    }
    @Test
    public void testDatatypeUUID() throws SQLException {
        testDatatype("myuuid", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "VARCHAR(2000000) UTF8");
    }
    @Test
    public void testDatatypeXML() throws SQLException {
        testDatatype("myxml", "<?xml version=\"1.0\"?><book><title>Manual</title><chapter>...</chapter></book>", "VARCHAR(2000000) UTF8");
    }

    public void testDatatype(final String columnName, final Object expectedValue, final String expectedType) throws SQLException {
        final StringBuilder query = new StringBuilder();
        query.append("SELECT ").append(columnName);
        query.append(" FROM ").append(VIRTUAL_SCHEMA).append(".").append(POSTGRES_TABLE_DATATYPES);
        query.append(" WHERE ").append(columnName).append(" IS NOT NULL");
        final ResultSet result = executeQuery(query.toString());
        result.next();
        Object actual = null;
        if (expectedValue.getClass() == BigDecimal.class) {
            actual = result.getBigDecimal(columnName);
        } else if (expectedValue.getClass() == Boolean.class) {
            actual = result.getBoolean(columnName);
        } else if (expectedValue.getClass() == String.class) {
            actual = result.getString(columnName);
        } else if (expectedValue.getClass() == Double.class) {
            actual = result.getDouble(columnName);
        } else if (expectedValue.getClass() == Integer.class) {
            actual = result.getInt(columnName);
        } else if (expectedValue.getClass() == Float.class) {
            actual = result.getFloat(columnName);
        }
        assertEquals(expectedValue, actual);
        assertEquals(expectedType, getColumnType(columnName, POSTGRES_TABLE_DATATYPES));
    }

    private String getColumnType(final String column, final String table) throws SQLException {
        if (this.columnTypes == null) {
            this.columnTypes = getColumnTypesOfTable(table);
        }
        return this.columnTypes.get(column.toUpperCase());
    }

    private Map<String, String> getColumnTypesOfTable(final String table) throws SQLException {
        final Map<String, String> map = new HashMap<>();
        final ResultSet result = executeQuery("DESCRIBE " + VIRTUAL_SCHEMA + "." + table);
        while (result.next()) {
            map.put(result.getString("COLUMN_NAME").toUpperCase(), result.getString("SQL_TYPE").toUpperCase());
        }
        return map;
    }

    private static void createPostgreSQLJDBCAdapter() throws SQLException, FileNotFoundException {
        final List<String> PostgreSQLIncludes = new ArrayList<>();
        PostgreSQLIncludes.add(getConfig().getJdbcAdapterPath());
        final String jdbcDriverPath = getConfig().getPostgresqlJdbcDriverPath();
        PostgreSQLIncludes.add(jdbcDriverPath);
        createJDBCAdapter(PostgreSQLIncludes);
    }

}