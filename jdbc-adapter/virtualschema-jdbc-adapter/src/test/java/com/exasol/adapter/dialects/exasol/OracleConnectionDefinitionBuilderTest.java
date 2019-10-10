package com.exasol.adapter.dialects.exasol;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.HashMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.ExaConnectionInformation;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.oracle.OracleConnectionDefinitionBuilder;
import com.exasol.adapter.dialects.oracle.OracleProperties;
import com.exasol.adapter.jdbc.*;

class OracleConnectionDefinitionBuilderTest extends AbstractConnectionDefinitionBuilderTest {
    private static final String ORACLE_CONNECTION_NAME = "ora_connection";

    @BeforeEach
    void beforeEach() {
        this.exaConnectionInformation = mock(ExaConnectionInformation.class);
        this.rawProperties = new HashMap<>();
    }

    @Override
    protected ConnectionDefinitionBuilder createConnectionBuilderUnderTest() {
        return new OracleConnectionDefinitionBuilder();
    }

    @Test
    void testBuildConnectionDefinitionForImportFromOraWithConnectionNameGiven() {
        mockExasolNamedConnection();
        setImportFromOraProperties();
        setConnectionNameProperty();
        assertThat(calculateConnectionDefinition(), equalTo("AT " + ORACLE_CONNECTION_NAME));
    }

    protected void setImportFromOraProperties() {
        setImportFromOraProperty();
        this.rawProperties.put(OracleProperties.ORACLE_CONNECTION_NAME_PROPERTY, ORACLE_CONNECTION_NAME);
    }

    private void setImportFromOraProperty() {
        this.rawProperties.put(OracleProperties.ORACLE_IMPORT_PROPERTY, "true");
    }

    @Test
    void testBuildConnectionDefinitionForImportFromOraWithConnectionStringUsernamePasswordGiven() {
        setImportFromOraProperties();
        setConnectionStringProperty("irrelevant");
        setUserNameProperty();
        setPasswordProperty();
        assertThat(calculateConnectionDefinition(),
                equalTo("AT " + ORACLE_CONNECTION_NAME + " USER '" + USER + "' IDENTIFIED BY '" + PW + "'"));
    }

    @Test
    void testBuildConnectionDefinitionForImportFromOraWithOnlyOracleConnectionName() {
        mockExasolNamedConnection();
        setImportFromOraProperties();
        assertThat(calculateConnectionDefinition(), equalTo("AT " + ORACLE_CONNECTION_NAME));
    }

    @Test
    void testBuildConnectionDefinitionWithExtraUsernameThrowsException() {
        setConnectionNameProperty();
        setUserNameProperty();
        assertIllegalPropertiesThrowsException(this.rawProperties);
    }

    @Test
    void testBuildConnectionDefinitionWithExtraPasswordThrowsException() {
        setConnectionNameProperty();
        setPasswordProperty();
        assertIllegalPropertiesThrowsException(this.rawProperties);
    }

    @Test
    void testBuildConnectionDefinitionWithExtraConnectionStringThrowsException() {
        setConnectionNameProperty();
        setConnectionStringProperty("irrelevant");
        assertIllegalPropertiesThrowsException(this.rawProperties);
    }

    @Test
    void testBuildConnectionDefinitionWithoutConnectionInfomationThrowsException() {
        setImportFromOraProperties();
        assertThrows(IllegalArgumentException.class, () -> new BaseConnectionDefinitionBuilder()
                .buildConnectionDefinition(new AdapterProperties(this.rawProperties), null));
    }

    @Test
    void testBuildConnectionDefinitionWithMissingOracleConnectionNameThrowsException() {
        setImportFromOraProperty();
        assertThrows(IllegalArgumentException.class, () -> new BaseConnectionDefinitionBuilder()
                .buildConnectionDefinition(new AdapterProperties(this.rawProperties), null));
    }
}