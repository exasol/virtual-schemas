package com.exasol.adapter.dialects.exasol;

import java.util.logging.Logger;

import com.exasol.ExaConnectionInformation;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.BaseConnectionDefinitionBuilder;

/**
 * This class implements an Exasol-specific connection definition builder.
 */
public class ExasolConnectionDefinitionBuilder extends BaseConnectionDefinitionBuilder {
    private static final Logger LOGGER = Logger.getLogger(ExasolConnectionDefinitionBuilder.class.getName());

    @Override
    public String buildConnectionDefinition(final AdapterProperties properties,
            final ExaConnectionInformation exaConnectionInformation) {
        if (properties.containsKey(ExasolProperties.EXASOL_IMPORT_PROPERTY)) {
            return buildImportFromExaConnectionDefinition(properties, exaConnectionInformation);
        } else {
            return super.buildConnectionDefinition(properties, exaConnectionInformation);
        }
    }

    private String buildImportFromExaConnectionDefinition(final AdapterProperties properties,
            final ExaConnectionInformation exaConnectionInformation) {
        if (properties.containsKey(ExasolProperties.EXASOL_CONNECTION_STRING_PROPERTY) && properties.hasUsername()
                && properties.hasPassword()) {
            return mixConnectionPropertiesWithExasolConnectionString(properties);
        } else if (properties.hasConnectionName()) {
            if (properties.hasUsername() || properties.hasPassword()) {
                return mixNamedConnectionWithPropertiesAndExasolConnectionString(properties, exaConnectionInformation);
            } else {
                return mixNamedConnectionWithExasolConnectionString(properties, exaConnectionInformation);
            }
        } else {
            throw new IllegalArgumentException(
                    "Incomplete remote connection information. Please specify an Exasol connection string with property "
                            + ExasolProperties.EXASOL_CONNECTION_STRING_PROPERTY + " plus a named connection with "
                            + AdapterProperties.CONNECTION_NAME_PROPERTY + " or individual connetion properties "
                            + AdapterProperties.CONNECTION_STRING_PROPERTY + ", " + AdapterProperties.USERNAME_PROPERTY
                            + " and " + AdapterProperties.PASSWORD_PROPERTY + ".");
        }
    }

    private String mixNamedConnectionWithExasolConnectionString(final AdapterProperties properties,
            final ExaConnectionInformation exaConnectionInformation) {
        final String exasolConnectionString = getExasolConnectionString(properties);
        LOGGER.finer(() -> "Mixing Exasol connection string \"" + exasolConnectionString + "\" into named connection.");
        return getConnectionDefinition(exasolConnectionString, exaConnectionInformation.getUser(),
                exaConnectionInformation.getPassword());
    }

    private String getExasolConnectionString(final AdapterProperties properties) {
        return properties.get(ExasolProperties.EXASOL_CONNECTION_STRING_PROPERTY);
    }

    private String mixConnectionPropertiesWithExasolConnectionString(final AdapterProperties properties) {
        LOGGER.warning(() -> "Defining credentials individually with properties is deprecated."
                + " Provide a connection name instead in property " + AdapterProperties.CONNECTION_NAME_PROPERTY + ".");
        return getConnectionDefinition(getExasolConnectionString(properties), properties.getUsername(),
                properties.getPassword());
    }

    private String mixNamedConnectionWithPropertiesAndExasolConnectionString(final AdapterProperties properties,
            final ExaConnectionInformation exaConnectionInformation) {
        LOGGER.warning(() -> "Overriding details of a named connection with individually with properties is deprecated."
                + " Provide only the connection name in property " + AdapterProperties.CONNECTION_NAME_PROPERTY + ".");
        final String username = properties.hasUsername() ? properties.getUsername()
                : exaConnectionInformation.getUser();
        final String password = properties.hasPassword() ? properties.getPassword()
                : exaConnectionInformation.getPassword();
        return getConnectionDefinition(getExasolConnectionString(properties), username, password);
    }
}