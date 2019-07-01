package com.exasol.adapter.jdbc;

import static com.exasol.adapter.AdapterProperties.*;

import java.util.logging.Logger;

import com.exasol.ExaConnectionInformation;
import com.exasol.adapter.AdapterProperties;

/**
 * This class creates the connection definition part of <code>IMPORT</code> statements.
 *
 * @see <a href="https://docs.exasol.com/sql/import.htm">IMPORT (Exasol documentation)</a>
 */
public class BaseConnectionDefinitionBuilder implements ConnectionDefinitionBuilder {
    private static final Logger LOGGER = Logger.getLogger(BaseConnectionDefinitionBuilder.class.getName());
    private static final String MISSING_CONNECTION_DETAILS_ERROR = "Incomplete remote connection information."
            + " Please specify either a named connection with " + CONNECTION_NAME_PROPERTY
            + " or individual connetion properties " + CONNECTION_STRING_PROPERTY + ", " + USERNAME_PROPERTY + " and "
            + PASSWORD_PROPERTY + ".";
    protected static final String CONFLICTING_CONNECTION_DETAILS_ERROR = "Mixing named connections in property "
            + CONNECTION_NAME_PROPERTY + " and individual conneciton properties " + CONNECTION_STRING_PROPERTY + ", "
            + USERNAME_PROPERTY + " and " + PASSWORD_PROPERTY + " is not allowed.";

    /**
     * Get the connection definition part of a push-down query.
     *
     * @param properties               user-defined adapter properties
     * @param exaConnectionInformation details of a named Exasol connection
     * @return credentials part of the push-down query
     */
    @Override
    public String buildConnectionDefinition(final AdapterProperties properties,
            final ExaConnectionInformation exaConnectionInformation) {
        if (hasIndividualConnectionPropertiesOnly(properties)) {
            return getConnectionFromPropertiesOnly(properties);
        } else if (hasConflictingConnectionProperties(properties)) {
            throw new IllegalArgumentException(CONFLICTING_CONNECTION_DETAILS_ERROR);
        } else if (properties.hasConnectionName()) {
            return getNamedConnection(properties);
        } else {
            throw new IllegalArgumentException(MISSING_CONNECTION_DETAILS_ERROR);
        }
    }

    private String getConnectionFromPropertiesOnly(final AdapterProperties properties) {
        warnConnectionPropertiesDeprecated();
        return getConnectionDefinition(properties.getConnectionString(), properties.getUsername(),
                properties.getPassword());
    }

    protected void warnConnectionPropertiesDeprecated() {
        LOGGER.warning(() -> "Defining credentials individually with properties is deprecated."
                + " Provide a connection name instead in property " + CONNECTION_NAME_PROPERTY + ".");
    }

    protected String getConnectionDefinition(final String connectionString, final String username,
            final String password) {
        return "AT '" + connectionString + "' USER '" + username + "' IDENTIFIED BY '" + password + "'";
    }

    protected boolean hasIndividualConnectionPropertiesOnly(final AdapterProperties properties) {
        return !properties.hasConnectionName() && properties.hasConnectionString() && properties.hasUsername()
                && properties.hasPassword();
    }

    protected boolean hasConflictingConnectionProperties(final AdapterProperties properties) {
        return properties.hasConnectionName()
                && (properties.hasConnectionString() || properties.hasUsername() || properties.hasPassword());
    }

    private String getNamedConnection(final AdapterProperties properties) {
        return "AT " + properties.getConnectionName();
    }
}