package com.exasol.adapter.jdbc;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.HashMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.ExaConnectionInformation;

class BaseConnectionDefinitionBuilderTest extends AbstractConnectionDefinitionBuilderTest {
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
    void testBuildConnectionDefinitionWithoutConnectionInfomationThrowsException() {
        assertIllegalPropertiesThrowsException(Collections.emptyMap());
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
}