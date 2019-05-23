package com.exasol.adapter.dialects;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.HashMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.ExaConnectionInformation;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.BaseConnectionDefinitionBuilder;
import com.exasol.adapter.jdbc.ConnectionDefinitionBuilder;

public class BaseConnectionDefinitionBuilderTest extends AbstractConnectionDefinitionBuilderTest {
    @BeforeEach
    void beforeEach() {
        this.exaConnectionInformation = mock(ExaConnectionInformation.class);
        this.rawProperties = new HashMap<>();
    }

    @Override
    protected ConnectionDefinitionBuilder createConnectionBuilderUnderTest() {
        return new BaseConnectionDefinitionBuilder();
    }

    @Test
    void testBuildConnectionDefinitionForJDBCImportWithConnectionNameGiven() {
        mockExasolNamedConnection();
        setConnectionNameProperty();
        assertThat(calculateConnectionDefinition(), equalTo("AT " + CONNECTION_NAME));
    }

    @Test
    void testBuildConnectionDefinitionForJDBCImportWithConnectionStringUsernamePasswordGiven() {
        setConnectionStringProperty(ADDRESS);
        setUserNameProperty();
        setPasswordProperty();
        assertThat(calculateConnectionDefinition(), equalTo(ADDRESS_WITH_USER_IDENTIFIED_BY));
    }

    @Test
    void testBuildConnectionDefinitionForJdbcImportWithNamedConnectionAndUsernameOverride() {
        mockExasolNamedConnection();
        setConnectionNameProperty();
        setUserNameProperty();
        assertThat(calculateConnectionDefinition(),
                equalTo("AT '" + CONNECTION_ADDRESS + "' USER '" + USER + "' IDENTIFIED BY '" + CONNECTION_PW + "'"));
    }

    @Test
    void testBuildConnectionDefinitionForJdbcImportWithNamedConnectionAndPasswordOverride() {
        mockExasolNamedConnection();
        setConnectionNameProperty();
        setPasswordProperty();
        assertThat(calculateConnectionDefinition(),
                equalTo("AT '" + CONNECTION_ADDRESS + "' USER '" + CONNECTION_USER + "' IDENTIFIED BY '" + PW + "'"));
    }

    @Test
    void testBuildConnectionDefinitionForJdbcImportWithNamedAddressOverride() {
        mockExasolNamedConnection();
        setConnectionNameProperty();
        final String connectionString = "jdbc:foobar";
        setConnectionStringProperty(connectionString);
        assertThat(calculateConnectionDefinition(), equalTo(
                "AT '" + connectionString + "' USER '" + CONNECTION_USER + "' IDENTIFIED BY '" + CONNECTION_PW + "'"));
    }

    @Test
    void testBuildConnectionDefinitionWithoutConnectionInfomationThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> new BaseConnectionDefinitionBuilder()
                .buildConnectionDefinition(AdapterProperties.emptyProperties(), null));
    }
}