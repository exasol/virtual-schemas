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
import com.exasol.adapter.dialects.AbstractConnectionDefinitionBuilderTest;
import com.exasol.adapter.jdbc.BaseConnectionDefinitionBuilder;
import com.exasol.adapter.jdbc.ConnectionDefinitionBuilder;

class ExasolConnectionDefinitionBuilderTest extends AbstractConnectionDefinitionBuilderTest {
    private static final String EXASOL_CONNECTION_STRING = "thehost:2345";

    @BeforeEach
    void beforeEach() {
        this.exaConnectionInformation = mock(ExaConnectionInformation.class);
        this.rawProperties = new HashMap<>();
    }

    @Override
    protected ConnectionDefinitionBuilder createConnectionBuilderUnderTest() {
        return new ExasolConnectionDefinitionBuilder();
    }

    @Test
    void testBuildConnectionDefinitionForImportFromExaWithConnectionNameGiven() {
        mockExasolNamedConnection();
        setImportFromExaProperties();
        setConnectionNameProperty();
        assertThat(calculateConnectionDefinition(), equalTo("AT '" + EXASOL_CONNECTION_STRING + "' USER '"
                + CONNECTION_USER + "' IDENTIFIED BY '" + CONNECTION_PW + "'"));
    }

    protected void setImportFromExaProperties() {
        this.rawProperties.put(ExasolProperties.EXASOL_IMPORT_PROPERTY, "true");
        this.rawProperties.put(ExasolProperties.EXASOL_CONNECTION_STRING_PROPERTY, EXASOL_CONNECTION_STRING);
    }

    @Test
    void testBuildConnectionDefinitionForImportFromExaWithConnectionStringUsernamePasswordGiven() {
        setImportFromExaProperties();
        setConnectionStringProperty("irrelevant");
        setUserNameProperty();
        setPasswordProperty();
        assertThat(calculateConnectionDefinition(),
                equalTo("AT '" + EXASOL_CONNECTION_STRING + "' USER '" + USER + "' IDENTIFIED BY '" + PW + "'"));
    }

    @Test
    void testBuildConnectionDefinitionForImportFromExaWithNamedConnectionAndUsernameOverride() {
        setImportFromExaProperties();
        mockExasolNamedConnection();
        setConnectionNameProperty();
        setUserNameProperty();
        assertThat(calculateConnectionDefinition(), equalTo(
                "AT '" + EXASOL_CONNECTION_STRING + "' USER '" + USER + "' IDENTIFIED BY '" + CONNECTION_PW + "'"));
    }

    @Test
    void testBuildConnectionDefinitionForImportFromExaWithNamedConnectionAndPasswordOverride() {
        setImportFromExaProperties();
        mockExasolNamedConnection();
        setConnectionNameProperty();
        setPasswordProperty();
        assertThat(calculateConnectionDefinition(), equalTo(
                "AT '" + EXASOL_CONNECTION_STRING + "' USER '" + CONNECTION_USER + "' IDENTIFIED BY '" + PW + "'"));
    }

    @Test
    void testBuildConnectionDefinitionWithoutConnectionInfomationThrowsException() {
        setImportFromExaProperties();
        assertThrows(IllegalArgumentException.class, () -> new BaseConnectionDefinitionBuilder()
                .buildConnectionDefinition(new AdapterProperties(this.rawProperties), null));
    }
}