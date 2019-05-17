package com.exasol.adapter.jdbc;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.ExaConnectionAccessException;
import com.exasol.ExaConnectionInformation;
import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.DummySqlDialect;
import com.exasol.adapter.dialects.SqlDialect;

public class ImportQueryBuilderTest {
    private static final String IMPORT_FROM_EXA_PROPERTY = "IMPORT_FROM_EXA";
    private static final String IMPORT_FROM_ORA_PROPERTY = "IMPORT_FROM_ORA";
    private static final String CONNECTION_NAME = "the_connection";
    private static final String USER = "the_user";
    private static final String PW = "top_secret";
    private static final String USER_IDENTIFIED_BY = "USER '" + USER + "' IDENTIFIED BY '" + PW + "'";
    private static final String ADDRESS = "the_address";
    private static final String ADDRESS_WITH_USER_IDENTIFIED_BY = "'" + ADDRESS + "' " + USER_IDENTIFIED_BY;
    private ExaConnectionInformation exaConnectionInformation;
    private ExaMetadata exaMetadata;

    @BeforeEach
    void beforeEach() {
        this.exaConnectionInformation = mock(ExaConnectionInformation.class);
        this.exaMetadata = mock(ExaMetadata.class);
    }

    @Test
    void testGetConnectionDefinitionForJDBCImportWithConnectionNameGiven() throws ExaConnectionAccessException {
        mockExasolNamedConnection();
        final Map<String, String> rawProperties = new HashMap<>();
        setConnectionNameProperty(rawProperties);
        assertThat(calculateCredentials(this.exaMetadata, rawProperties), equalTo(CONNECTION_NAME));
    }

    private void mockExasolNamedConnection() throws ExaConnectionAccessException {
        when(this.exaMetadata.getConnection(any())).thenReturn(this.exaConnectionInformation);
        when(this.exaConnectionInformation.getUser()).thenReturn(USER);
        when(this.exaConnectionInformation.getPassword()).thenReturn(PW);
        when(this.exaConnectionInformation.getAddress()).thenReturn(ADDRESS);
    }

    private String calculateCredentials(final ExaMetadata exaMetadata, final Map<String, String> rawProperties) {
        final AdapterProperties properties = new AdapterProperties(rawProperties);
        final SqlDialect dialect = new DummySqlDialect(null, properties);
        return new ImportQueryBuilder() //
                .dialect(dialect) //
                .properties(properties) //
                .getConnectionDefinition();
    }

    @Test
    public void testGetConnectionDefinitionForORAImportWithConnectionNameGiven() throws ExaConnectionAccessException {
        final Map<String, String> rawProperties = new HashMap<>();
        setConnectionNameProperty(rawProperties);
        setOracleImportProperty(rawProperties);
        assertThat(calculateCredentials(this.exaMetadata, rawProperties), equalTo(CONNECTION_NAME));
    }

    private void setConnectionNameProperty(final Map<String, String> rawProperties) {
        rawProperties.put(AdapterProperties.CONNECTION_NAME_PROPERTY, CONNECTION_NAME);
    }

    private void setOracleImportProperty(final Map<String, String> rawProperties) {
        rawProperties.put(IMPORT_FROM_ORA_PROPERTY, "true");
    }

    @Test
    void testGetConnectionDefinitionForEXAImportWithConnectionNameGiven() throws ExaConnectionAccessException {
        final Map<String, String> rawProperties = new HashMap<>();
        setConnectionNameProperty(rawProperties);
        rawProperties.put(IMPORT_FROM_EXA_PROPERTY, "true");
        assertThat(calculateCredentials(this.exaMetadata, rawProperties), equalTo(CONNECTION_NAME));
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
    public void testGetConnectionDefinitionForJDBCImportWithConnectionStringUsernamePasswordGiven() {
        final Map<String, String> rawProperties = createUsernameAndPasswordProperties();
        assertThat(calculateCredentials(this.exaMetadata, rawProperties), equalTo(ADDRESS_WITH_USER_IDENTIFIED_BY));
    }

    @Test
    void testGetConnectionDefinitionForORAImportWithConnectionStringUsernamePasswordGiven() {
        final Map<String, String> rawProperties = createUsernameAndPasswordProperties();
        setOracleImportProperty(rawProperties);
        assertThat(calculateCredentials(this.exaMetadata, rawProperties), equalTo(ADDRESS_WITH_USER_IDENTIFIED_BY));
    }

    @Test
    void testGetConnectionDefinitionForEXAImportWithConnectionStringUsernamePasswordGiven() {
        final Map<String, String> rawProperties = createUsernameAndPasswordProperties();
        rawProperties.put(IMPORT_FROM_EXA_PROPERTY, "true");
        assertThat(calculateCredentials(this.exaMetadata, rawProperties), equalTo(ADDRESS_WITH_USER_IDENTIFIED_BY));
    }

    @Test
    void testGetConnectionDefinitionForJdbcImportWithNamedConnectionAndUsernamePasswordOverride() {
        final Map<String, String> rawProperties = new HashMap<>();
        setConnectionNameProperty(rawProperties);
        setUserNameProperty(rawProperties);
        setPasswordProperty(rawProperties);
        assertThat(calculateCredentials(this.exaMetadata, rawProperties),
                equalTo(CONNECTION_NAME + " " + USER_IDENTIFIED_BY));
    }

    @Test
    void testGetConnectionDefinitionWithMissingPasswordInCredentialOverrideThrowsException() {
        final Map<String, String> rawProperties = new HashMap<>();
        setConnectionNameProperty(rawProperties);
        setUserNameProperty(rawProperties);
        assertThrows(IllegalArgumentException.class, () -> calculateCredentials(this.exaMetadata, rawProperties));
    }

    @Test
    void testGetConnectionDefinitionWithMissingUsernameInCredentialOverrideThrowsException() {
        final Map<String, String> rawProperties = new HashMap<>();
        setConnectionNameProperty(rawProperties);
        setPasswordProperty(rawProperties);
        assertThrows(IllegalArgumentException.class, () -> calculateCredentials(this.exaMetadata, rawProperties));
    }

    @Test
    void testGetConnectionDefinitionPlusAddressFromPropertiesThrowsException() {
        final Map<String, String> rawProperties = new HashMap<>();
        setConnectionNameProperty(rawProperties);
        setAddressProperty(rawProperties);
        assertThrows(IllegalArgumentException.class, () -> calculateCredentials(this.exaMetadata, rawProperties));
    }
}