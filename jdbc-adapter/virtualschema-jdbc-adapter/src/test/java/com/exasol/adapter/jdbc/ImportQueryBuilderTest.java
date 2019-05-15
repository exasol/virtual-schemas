package com.exasol.adapter.jdbc;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
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
    private static final String ADDRESS = "jdbc://dummy";
    private ExaConnectionInformation exaConnectionInformation;
    private ExaMetadata exaMetadata;

    @BeforeEach
    void beforeEach() {
        this.exaConnectionInformation = mock(ExaConnectionInformation.class);
        this.exaMetadata = mock(ExaMetadata.class);
    }

    @Test
    void getCredentialsForJDBCImportWithConnectionNameGiven() throws ExaConnectionAccessException {
        mockExasolNamedConnection();
        final Map<String, String> rawProperties = new HashMap<>();
        rawProperties.put(AdapterProperties.CONNECTION_NAME_PROPERTY, CONNECTION_NAME);
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
        final ImportQueryBuilder builder = new ImportQueryBuilder(dialect, exaMetadata);
        final String credentials = builder.getCredentialsForPushdownQuery(exaMetadata, properties);
        return credentials;
    }

    @Test
    public void getCredentialsForORAImportWithConnectionNameGiven() throws ExaConnectionAccessException {
        mockExasolNamedConnection();
        final Map<String, String> rawProperties = new HashMap<>();
        rawProperties.put(IMPORT_FROM_ORA_PROPERTY, "true");
        rawProperties.put(AdapterProperties.CONNECTION_NAME_PROPERTY, CONNECTION_NAME);
        assertThat(calculateCredentials(this.exaMetadata, rawProperties), equalTo(CONNECTION_NAME));
    }

    @Test
    void getCredentialsForEXAImportWithConnectionNameGiven() throws ExaConnectionAccessException {
        mockExasolNamedConnection();
        final Map<String, String> rawProperties = new HashMap<>();
        rawProperties.put(IMPORT_FROM_EXA_PROPERTY, "true");
        assertThat(calculateCredentials(this.exaMetadata, rawProperties), equalTo(CONNECTION_NAME));
    }

    private Map<String, String> createUsernameAndPasswordProperties() {
        final Map<String, String> rawProperties = new HashMap<>();
        rawProperties.put(AdapterProperties.USERNAME_PROPERTY, USER);
        rawProperties.put(AdapterProperties.PASSWORD_PROPERTY, PW);
        rawProperties.put(AdapterProperties.CONNECTION_STRING_PROPERTY, ADDRESS);
        return rawProperties;
    }

    @Test
    public void getCredentialsForJDBCImportWithConnectionStringUsernamePasswordGiven() {
        final Map<String, String> rawProperties = createUsernameAndPasswordProperties();
        assertThat(calculateCredentials(this.exaMetadata, rawProperties),
                equalTo("'" + ADDRESS + "' " + USER_IDENTIFIED_BY));
    }

    @Test
    public void getCredentialsForORAImportWithConnectionStringUsernamePasswordGiven() {
        final Map<String, String> rawProperties = createUsernameAndPasswordProperties();
        rawProperties.put(IMPORT_FROM_ORA_PROPERTY, "true");
        assertThat(calculateCredentials(this.exaMetadata, rawProperties), equalTo(USER_IDENTIFIED_BY));
    }

    @Test
    public void getCredentialsForEXAImportWithConnectionStringUsernamePasswordGiven() {
        final Map<String, String> rawProperties = createUsernameAndPasswordProperties();
        rawProperties.put(IMPORT_FROM_EXA_PROPERTY, "true");
        assertThat(calculateCredentials(this.exaMetadata, rawProperties), equalTo(USER_IDENTIFIED_BY));
    }
}