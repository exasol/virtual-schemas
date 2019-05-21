package com.exasol.adapter.jdbc;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.ExaConnectionAccessException;
import com.exasol.ExaConnectionInformation;
import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.DummySqlDialect;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.exasol.ExasolSqlDialect;
import com.exasol.adapter.sql.DummySqlStatement;

public class ImportQueryBuilderTest {
    private static final String IMPORT_FROM_EXA_PROPERTY = "IMPORT_FROM_EXA";
    private static final String IMPORT_FROM_ORA_PROPERTY = "IMPORT_FROM_ORA";
    private static final String USER = "property_user";
    private static final String PW = "property_secret";
    private static final String USER_IDENTIFIED_BY = "USER '" + USER + "' IDENTIFIED BY '" + PW + "'";
    private static final String ADDRESS = "property_address";
    private static final String ADDRESS_WITH_USER_IDENTIFIED_BY = "'" + ADDRESS + "' " + USER_IDENTIFIED_BY;
    private static final String CONNECTION_NAME = "the_connection";
    private static final String CONNECTION_USER = "connection_user";
    private static final String CONNECTION_PW = "connection_secret";
    private static final String CONNECTION_ADDRESS = "connection_address";
    private static final String CONNECTION_ADDRESS_USER_IDENTIFIED_BY = "'" + CONNECTION_ADDRESS + "' USER '"
            + CONNECTION_USER + "' IDENTIFIED BY '" + CONNECTION_PW + "'";
    private ExaConnectionInformation exaConnectionInformation;
    private ExaMetadata exaMetadata;

    @BeforeEach
    void beforeEach() {
        this.exaConnectionInformation = mock(ExaConnectionInformation.class);
        this.exaMetadata = mock(ExaMetadata.class);
    }

    @Test
    void testGetConnectionDefinitionForJDBCImportWithConnectionNameGiven()
            throws ExaConnectionAccessException, AdapterException {
        mockExasolNamedConnection();
        final Map<String, String> rawProperties = new HashMap<>();
        setConnectionNameProperty(rawProperties);
        assertThat(calculateCredentials(rawProperties), equalTo(CONNECTION_ADDRESS_USER_IDENTIFIED_BY));
    }

    private void mockExasolNamedConnection() throws ExaConnectionAccessException {
        when(this.exaMetadata.getConnection(any())).thenReturn(this.exaConnectionInformation);
        when(this.exaConnectionInformation.getUser()).thenReturn(CONNECTION_USER);
        when(this.exaConnectionInformation.getPassword()).thenReturn(CONNECTION_PW);
        when(this.exaConnectionInformation.getAddress()).thenReturn(CONNECTION_ADDRESS);
    }

    private void setConnectionNameProperty(final Map<String, String> rawProperties) {
        rawProperties.put(AdapterProperties.CONNECTION_NAME_PROPERTY, CONNECTION_NAME);
    }

    private String calculateCredentials(final Map<String, String> rawProperties) throws AdapterException {
        final AdapterProperties properties = new AdapterProperties(rawProperties);
        final SqlDialect dialect = new DummySqlDialect(null, properties);
        return new ImportQueryBuilder() //
                .dialect(dialect) //
                .properties(properties) //
                .exaMetadata(this.exaMetadata) //
                .getConnectionDefinition();
    }

    @Test
    public void testGetConnectionDefinitionForORAImportWithConnectionNameGiven()
            throws ExaConnectionAccessException, AdapterException {
        mockExasolNamedConnection();
        final Map<String, String> rawProperties = new HashMap<>();
        setConnectionNameProperty(rawProperties);
        setOracleImportProperty(rawProperties);
        assertThat(calculateCredentials(rawProperties), equalTo(CONNECTION_ADDRESS_USER_IDENTIFIED_BY));
    }

    private void setOracleImportProperty(final Map<String, String> rawProperties) {
        rawProperties.put(IMPORT_FROM_ORA_PROPERTY, "true");
    }

    @Test
    void testGetConnectionDefinitionForEXAImportWithConnectionNameGiven()
            throws ExaConnectionAccessException, AdapterException {
        mockExasolNamedConnection();
        final Map<String, String> rawProperties = new HashMap<>();
        setConnectionNameProperty(rawProperties);
        setExasolImportProperty(rawProperties);
        assertThat(calculateCredentials(rawProperties), equalTo(CONNECTION_ADDRESS_USER_IDENTIFIED_BY));
    }

    private Map<String, String> createUsernameAndPasswordProperties() {
        final Map<String, String> rawProperties = new HashMap<>();
        setUserNameProperty(rawProperties);
        setPasswordProperty(rawProperties);
        setAddressProperty(rawProperties);
        return rawProperties;
    }

    private void setUserNameProperty(final Map<String, String> rawProperties) {
        rawProperties.put(AdapterProperties.USERNAME_PROPERTY, USER);
    }

    private void setPasswordProperty(final Map<String, String> rawProperties) {
        rawProperties.put(AdapterProperties.PASSWORD_PROPERTY, PW);
    }

    private void setAddressProperty(final Map<String, String> rawProperties) {
        rawProperties.put(AdapterProperties.CONNECTION_STRING_PROPERTY, ADDRESS);
    }

    @Test
    public void testGetConnectionDefinitionForJDBCImportWithConnectionStringUsernamePasswordGiven()
            throws AdapterException {
        final Map<String, String> rawProperties = createUsernameAndPasswordProperties();
        assertThat(calculateCredentials(rawProperties), equalTo(ADDRESS_WITH_USER_IDENTIFIED_BY));
    }

    @Test
    void testGetConnectionDefinitionForORAImportWithConnectionStringUsernamePasswordGiven() throws AdapterException {
        final Map<String, String> rawProperties = createUsernameAndPasswordProperties();
        setOracleImportProperty(rawProperties);
        assertThat(calculateCredentials(rawProperties), equalTo(ADDRESS_WITH_USER_IDENTIFIED_BY));
    }

    @Test
    void testGetConnectionDefinitionForEXAImportWithConnectionStringUsernamePasswordGiven() throws AdapterException {
        final Map<String, String> rawProperties = createUsernameAndPasswordProperties();
        setExasolImportProperty(rawProperties);
        assertThat(calculateCredentials(rawProperties), equalTo(ADDRESS_WITH_USER_IDENTIFIED_BY));
    }

    private void setExasolImportProperty(final Map<String, String> rawProperties) {
        rawProperties.put(IMPORT_FROM_EXA_PROPERTY, "true");
    }

    @Test
    void testGetConnectionDefinitionForJdbcImportWithNamedConnectionAndUsernamePasswordOverride()
            throws AdapterException, ExaConnectionAccessException {
        mockExasolNamedConnection();
        final Map<String, String> rawProperties = new HashMap<>();
        setConnectionNameProperty(rawProperties);
        setUserNameProperty(rawProperties);
        setPasswordProperty(rawProperties);
        assertThat(calculateCredentials(rawProperties), equalTo("'" + CONNECTION_ADDRESS + "' " + USER_IDENTIFIED_BY));
    }

    @Test
    void testBuildWithJdbcConnection() throws AdapterException, SQLException, ExaConnectionAccessException {
        mockExasolNamedConnection();
        final Connection connectionMock = mockConnection();
        final Map<String, String> rawProperties = new HashMap<>();
        setConnectionNameProperty(rawProperties);
        final AdapterProperties properties = new AdapterProperties(rawProperties);
        assertThat(calculateImportQuery(connectionMock, properties, new DummySqlDialect(connectionMock, properties)),
                equalTo("IMPORT INTO (c1 DECIMAL(18, 0)) FROM JDBC AT " + CONNECTION_ADDRESS_USER_IDENTIFIED_BY
                        + " STATEMENT 'SELECT 1 FROM DUAL'"));
    }

    private Connection mockConnection() throws SQLException {
        final ResultSetMetaData metadataMock = mock(ResultSetMetaData.class);
        when(metadataMock.getColumnCount()).thenReturn(1);
        when(metadataMock.getColumnType(1)).thenReturn(Types.INTEGER);
        final PreparedStatement statementMock = mock(PreparedStatement.class);
        when(statementMock.getMetaData()).thenReturn(metadataMock);
        final Connection connectionMock = mock(Connection.class);
        when(connectionMock.prepareStatement(any())).thenReturn(statementMock);
        return connectionMock;
    }

    private String calculateImportQuery(final Connection connectionMock, final AdapterProperties properties,
            final SqlDialect dialect) throws AdapterException, SQLException {
        final String query = new ImportQueryBuilder() //
                .dialect(dialect) //
                .properties(properties) //
                .statement(new DummySqlStatement()) //
                .exaMetadata(this.exaMetadata) //
                .build();
        return query;
    }

    @Test
    void testBuildWithExasolConnection() throws AdapterException, SQLException, ExaConnectionAccessException {
        mockExasolNamedConnection();
        final Connection connectionMock = mockConnection();
        final Map<String, String> rawProperties = new HashMap<>();
        setConnectionNameProperty(rawProperties);
        setExasolImportProperty(rawProperties);
        final AdapterProperties properties = new AdapterProperties(rawProperties);
        final SqlDialect dialect = new ExasolSqlDialect(connectionMock, properties);
        assertThat(calculateImportQuery(connectionMock, properties, dialect), equalTo(
                "IMPORT FROM EXA AT " + CONNECTION_ADDRESS_USER_IDENTIFIED_BY + " STATEMENT 'SELECT 1 FROM DUAL'"));
    }
}