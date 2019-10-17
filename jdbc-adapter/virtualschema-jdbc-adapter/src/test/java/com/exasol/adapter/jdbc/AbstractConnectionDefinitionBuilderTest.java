package com.exasol.adapter.jdbc;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import com.exasol.ExaConnectionInformation;
import com.exasol.adapter.AdapterProperties;

public abstract class AbstractConnectionDefinitionBuilderTest {
    protected static final String USER = "property_user";
    protected static final String PW = "property_secret";
    protected static final String USER_IDENTIFIED_BY = "USER '" + USER + "' IDENTIFIED BY '" + PW + "'";
    protected static final String ADDRESS = "property_address";
    protected static final String ADDRESS_WITH_USER_IDENTIFIED_BY = "AT '" + ADDRESS + "' " + USER_IDENTIFIED_BY;
    protected static final String CONNECTION_NAME = "the_connection";
    protected static final String CONNECTION_USER = "connection_user";
    protected static final String CONNECTION_PW = "connection_secret";
    protected static final String CONNECTION_ADDRESS = "connection_address";
    protected ExaConnectionInformation exaConnectionInformation;
    protected Map<String, String> rawProperties;

    abstract protected ConnectionDefinitionBuilder createConnectionBuilderUnderTest();

    protected void mockExasolNamedConnection() {
        when(this.exaConnectionInformation.getUser()).thenReturn(CONNECTION_USER);
        when(this.exaConnectionInformation.getPassword()).thenReturn(CONNECTION_PW);
        when(this.exaConnectionInformation.getAddress()).thenReturn(CONNECTION_ADDRESS);
    }

    protected Map<String, String> createUsernameAndPasswordProperties() {
        final Map<String, String> rawProperties = new HashMap<>();
        setUserNameProperty();
        setPasswordProperty();
        setAddressProperty();
        return rawProperties;
    }

    protected void setUserNameProperty() {
        this.rawProperties.put(AdapterProperties.USERNAME_PROPERTY, USER);
    }

    protected void setPasswordProperty() {
        this.rawProperties.put(AdapterProperties.PASSWORD_PROPERTY, PW);
    }

    private void setAddressProperty() {
        this.rawProperties.put(AdapterProperties.CONNECTION_STRING_PROPERTY, ADDRESS);
    }

    protected void setConnectionStringProperty(final String connectionString) {
        this.rawProperties.put(AdapterProperties.CONNECTION_STRING_PROPERTY, connectionString);
    }

    protected void setConnectionNameProperty() {
        this.rawProperties.put(AdapterProperties.CONNECTION_NAME_PROPERTY, CONNECTION_NAME);
    }

    protected String calculateConnectionDefinition() {
        final AdapterProperties properties = new AdapterProperties(this.rawProperties);
        return createConnectionBuilderUnderTest().buildConnectionDefinition(properties, this.exaConnectionInformation);
    }

    protected void assertIllegalPropertiesThrowsException(final Map<String, String> rawProperties) {
        assertThrows(IllegalArgumentException.class, () -> new BaseConnectionDefinitionBuilder()
                .buildConnectionDefinition(new AdapterProperties(rawProperties), null));
    }
}