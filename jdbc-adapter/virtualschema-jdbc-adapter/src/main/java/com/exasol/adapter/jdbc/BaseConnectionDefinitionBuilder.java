package com.exasol.adapter.jdbc;

import java.util.logging.Logger;

import com.exasol.ExaConnectionInformation;
import com.exasol.adapter.AdapterProperties;

/**
 * This class creates the connection definition part of <code>IMPORT</code>
 * statements.
 *
 * @see <a href="https://docs.exasol.com/sql/import.htm">IMPORT (Exasol
 *      documentation)</a>
 */
public class BaseConnectionDefinitionBuilder implements ConnectionDefinitionBuilder {
    private static final Logger LOGGER = Logger.getLogger(BaseConnectionDefinitionBuilder.class.getName());

    /**
     * Get the connection definition part of a push-down query
     *
     * @param properties               user-defined adapter properties
     * @param exaConnectionInformation details of a named Exasol connection
     * @return credentials part of the push-down query
     */
    @Override
    public String buildConnectionDefinition(final AdapterProperties properties,
            final ExaConnectionInformation exaConnectionInformation) {
        if (properties.hasConnectionString() && properties.hasUsername() && properties.hasPassword()) {
            return getConnectionFromPropertiesOnly(properties);
        } else if (properties.hasConnectionName()) {
            if (properties.hasConnectionString() || properties.hasUsername() || properties.hasPassword()) {
                return mixNamedConnectionWithProperties(properties, exaConnectionInformation);
            } else {
                return getNamedConnection(properties);
            }
        } else {
            throw new IllegalArgumentException(
                    "Incomplete remote connection information. Please specify at least a named connection with "
                            + AdapterProperties.CONNECTION_NAME_PROPERTY + " or individual connetion properties "
                            + AdapterProperties.CONNECTION_STRING_PROPERTY + ", " + AdapterProperties.USERNAME_PROPERTY
                            + " and " + AdapterProperties.PASSWORD_PROPERTY + ".");
        }
    }

    private String getNamedConnection(final AdapterProperties properties) {
        return "AT " + properties.getConnectionName();
    }

    private String getConnectionFromPropertiesOnly(final AdapterProperties properties) {
        LOGGER.warning(() -> "Defining credentials individually with properties is deprecated."
                + " Provide a connection name instead in property " + AdapterProperties.CONNECTION_NAME_PROPERTY + ".");
        return getConnectionDefinition(properties.getConnectionString(), properties.getUsername(),
                properties.getPassword());
    }

    private String mixNamedConnectionWithProperties(final AdapterProperties properties,
            final ExaConnectionInformation exaConnectionInformation) {
        LOGGER.warning(() -> "Overriding details of a named connection with individually with properties is deprecated."
                + " Provide only the connection name in property " + AdapterProperties.CONNECTION_NAME_PROPERTY + ".");
        final String connectionString = properties.hasConnectionString() ? properties.getConnectionString()
                : exaConnectionInformation.getAddress();
        final String username = properties.hasUsername() ? properties.getUsername()
                : exaConnectionInformation.getUser();
        final String password = properties.hasPassword() ? properties.getPassword()
                : exaConnectionInformation.getPassword();
        return getConnectionDefinition(connectionString, username, password);
    }

    protected String getConnectionDefinition(final String connectionString, final String username,
            final String password) {
        return "AT '" + connectionString + "' USER '" + username + "' IDENTIFIED BY '" + password + "'";
    }
}