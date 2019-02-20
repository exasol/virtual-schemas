package com.exasol.adapter.dialects.impl;

import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.exasol.adapter.AdapterException;
import com.exasol.jdbc.DataException;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import com.exasol.adapter.dialects.AbstractIntegrationTest;
import org.junit.rules.ExpectedException;

/**
 * Integration test for the PostgreSQL SQL dialect
 *
 */
public class PostgreSQLDialectIT extends AbstractIntegrationTest {

    private static final String VIRTUAL_SCHEMA = "PG";
    private static final String VIRTUAL_SCHEMA_UPPERCASE_TABLE = "PG_UPPER";
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
                IS_LOCAL, getConfig().debugAddress(), "", null);
        createVirtualSchema(VIRTUAL_SCHEMA_UPPERCASE_TABLE, PostgreSQLSqlDialect.getPublicName(), POSTGRES_CATALOG, POSTGRES_SCHEMA_UPPERCASE_TABLE, "",
                getConfig().getPostgresqlUser(), getConfig().getPostgresqlPassword(), "ADAPTER.JDBC_ADAPTER",
                getConfig().getPostgresqlDockerJdbcConnectionString(), IS_LOCAL, getConfig().debugAddress(), "",
                "ignore_errors='POSTGRESQL_UPPERCASE_TABLES'");
    }

    private static void createTestSchema() throws SQLException, ClassNotFoundException, FileNotFoundException {
        final String user = getConfig().getPostgresqlUser();
        final String password = getConfig().getPostgresqlPassword();
        final String postgresConnectionString = getConfig().getPostgresqlJdbcConnectionString();
        Class.forName("org.postgresql.Driver");
        try (Connection conn = DriverManager.getConnection(postgresConnectionString, user, password))
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
            stmt.execute(String.format("INSERT INTO %s.%s(mydate) VALUES(current_date)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
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
            stmt.execute(String.format("INSERT INTO %s.%s(mytime) VALUES(current_time)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mytimeWithTimeZone) VALUES(current_time)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mytimestamp) VALUES(current_timestamp)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mytimestampwithtimezone) VALUES(current_timestamp)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mytsquery) VALUES('fat & rat'::tsquery)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(mytsvector) VALUES(to_tsvector('english', 'The Fat Rats'))", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(myuuid) VALUES('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid)", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
            stmt.execute(String.format("INSERT INTO %s.%s(myxml) VALUES(XMLPARSE (DOCUMENT '<?xml version=\"1.0\"?><book><title>Manual</title><chapter>...</chapter></book>'))", POSTGRES_SCHEMA, POSTGRES_TABLE_DATATYPES));
        }
    }

    private static void createTestSchemaUpperCaseTable() throws SQLException, ClassNotFoundException, FileNotFoundException {
        final String user = getConfig().getPostgresqlUser();
        final String password = getConfig().getPostgresqlPassword();
        final String postgresConnectionString = getConfig().getPostgresqlJdbcConnectionString();
        Class.forName("org.postgresql.Driver");
        try (Connection conn = DriverManager.getConnection(postgresConnectionString, user, password))
        {
            final Statement stmt = conn.createStatement();
            stmt.execute(String.format("create schema %s", POSTGRES_SCHEMA_UPPERCASE_TABLE));
            stmt.execute(String.format("create table %s.lower_t(x int, y int)", POSTGRES_SCHEMA_UPPERCASE_TABLE));
            stmt.execute(String.format("create table %s.\"UPPer_t\"(x int, y int)", POSTGRES_SCHEMA_UPPERCASE_TABLE));
        }
    }

    @Test
    public void testOpenVirtualSchema() throws SQLException, ClassNotFoundException, FileNotFoundException {
        final String open_schema_query = String.format("OPEN SCHEMA %s", VIRTUAL_SCHEMA);
        execute(open_schema_query);
    }

    @Test
    public void testSelectSingleColumn() throws SQLException, ClassNotFoundException, FileNotFoundException {
        final String query = String.format("SELECT x FROM %s.t", VIRTUAL_SCHEMA);
        final ResultSet result = executeQuery(query);
        matchNextRow(result, 1L);
    }

    @Test
    public void testCreateSchemaWithUpperCaseTables() throws SQLException, ClassNotFoundException, FileNotFoundException {
        expectedEx.expect(DataException.class);
        expectedEx.expectMessage("Table UPPer_t cannot be used in virtual schema. Set property IGNORE_ERRORS to POSTGRESQL_UPPERCASE_TABLES to enforce schema creation.");
        createVirtualSchema("FOO", PostgreSQLSqlDialect.getPublicName(), POSTGRES_CATALOG, POSTGRES_SCHEMA_UPPERCASE_TABLE, "",
                getConfig().getPostgresqlUser(), getConfig().getPostgresqlPassword(), "ADAPTER.JDBC_ADAPTER",
                getConfig().getPostgresqlDockerJdbcConnectionString(), IS_LOCAL, getConfig().debugAddress(), "", null);
    }

    @Test
    public void testQueryUpperCaseTableQuoted() throws SQLException, ClassNotFoundException, FileNotFoundException {
        expectedEx.expect(DataException.class);
        expectedEx.expectMessage("Cannot resolve column types");
        final String query = String.format("SELECT x FROM  %s.\"UPPer_t\"", VIRTUAL_SCHEMA_UPPERCASE_TABLE);
        final ResultSet result = executeQuery(query);
    }

    @Test
    public void testQueryUpperCaseTable() throws SQLException, ClassNotFoundException, FileNotFoundException {
        expectedEx.expect(SQLException.class);
        expectedEx.expectMessage("object PG_UPPER.UPPER_T not found");
        final String query = String.format("SELECT x FROM  %s.UPPer_t", VIRTUAL_SCHEMA_UPPERCASE_TABLE);
        final ResultSet result = executeQuery(query);
    }

    @Test
    public void testQueryLowerCaseTable() throws SQLException, ClassNotFoundException, FileNotFoundException {
        final String query = String.format("SELECT x FROM  %s.lower_t", VIRTUAL_SCHEMA_UPPERCASE_TABLE);
        final ResultSet result = executeQuery(query);
    }

    @Test
    public void testUnsetIgnoreUpperCaseTablesAndRefresh() throws SQLException, ClassNotFoundException, FileNotFoundException {
        expectedEx.expect(DataException.class);
        expectedEx.expectMessage("Table UPPer_t cannot be used in virtual schema. Set property IGNORE_ERRORS to POSTGRESQL_UPPERCASE_TABLES to enforce schema creation.");
        final String alter_schema_query = String.format("ALTER VIRTUAL SCHEMA %s set ignore_errors=''", VIRTUAL_SCHEMA_UPPERCASE_TABLE);
        execute(alter_schema_query);
        final String refresh_schema_query = String.format("ALTER VIRTUAL SCHEMA %s REFRESH", VIRTUAL_SCHEMA_UPPERCASE_TABLE);
        execute(refresh_schema_query);
    }

    @Test
    public void testSetIgnoreUpperCaseTablesAndRefresh() throws SQLException, ClassNotFoundException, FileNotFoundException {
        final String alter_schema_query = String.format("ALTER VIRTUAL SCHEMA %s set ignore_errors='POSTGRESQL_UPPERCASE_TABLES'", VIRTUAL_SCHEMA_UPPERCASE_TABLE);
        execute(alter_schema_query);
        final String refresh_schema_query = String.format("ALTER VIRTUAL SCHEMA %s REFRESH", VIRTUAL_SCHEMA_UPPERCASE_TABLE);
        execute(refresh_schema_query);
    }

    @Test
    public void testDatatypeBigint() throws SQLException {
        testDatatype("mybigint", "10000000000", "DECIMAL(19,0)");
    }
    @Test
    public void testDatatypeBigSerial() throws SQLException {
        testDatatype("mybigserial", "1", "DECIMAL(19,0)");
    }

    public void testDatatype(final String columnName, final String expectedValue, final String expectedType) throws SQLException {
        final String query = String.format("SELECT %s FROM %s.%s", columnName, VIRTUAL_SCHEMA, POSTGRES_TABLE_DATATYPES);
        final ResultSet result = executeQuery(query);
        result.next();
        final BigDecimal expected = new BigDecimal(expectedValue);
        final BigDecimal actual = result.getBigDecimal(columnName);
        assertEquals(expected, actual);
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