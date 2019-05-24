package com.exasol.adapter.dialects.oracle;

import static com.exasol.adapter.AdapterProperties.CONNECTION_NAME_PROPERTY;
import static com.exasol.adapter.dialects.oracle.OracleProperties.ORACLE_CONNECTION_NAME_PROPERTY;
import static com.exasol.adapter.dialects.oracle.OracleProperties.ORACLE_IMPORT_PROPERTY;

import java.util.logging.Logger;

import com.exasol.ExaConnectionInformation;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.BaseConnectionDefinitionBuilder;

/**
 * This class implements an Oracle-specific connection definition builder
 */
public class OracleConnectionDefinitionBuilder extends BaseConnectionDefinitionBuilder {
    private static final Logger LOGGER = Logger.getLogger(OracleConnectionDefinitionBuilder.class.getName());

    @Override
    public String buildConnectionDefinition(final AdapterProperties properties,
            final ExaConnectionInformation exaConnectionInformation) {
        if (properties.containsKey(ORACLE_IMPORT_PROPERTY)) {
            return buildImportFromOraConnectionDefinition(properties);
        } else {
            return super.buildConnectionDefinition(properties, exaConnectionInformation);
        }
    }

    private String buildImportFromOraConnectionDefinition(final AdapterProperties properties) {
        if (properties.containsKey(ORACLE_CONNECTION_NAME_PROPERTY) && properties.hasUsername()
                && properties.hasPassword()) {
            return mixConnectionPropertiesWithOracleConnectionName(properties);
        } else if (properties.hasConnectionName()) {
            LOGGER.warning("Exasol named conneciton set in property " + AdapterProperties.CONNECTION_NAME_PROPERTY
                    + " is ignored during IMPORT FROM ORA.");
            return buildOracleConnectionDefinition(properties);
        } else {
            return buildOracleConnectionDefinition(properties);
        }
    }

    private String getOracleConnectionDefinition(final String oracleConnectionName, final String username,
            final String password) {
        return "AT " + oracleConnectionName + " USER '" + username + "' IDENTIFIED BY '" + password + "'";
    }

    private String getOracleConnectionName(final AdapterProperties properties) {
        return properties.get(ORACLE_CONNECTION_NAME_PROPERTY);
    }

    private String mixConnectionPropertiesWithOracleConnectionName(final AdapterProperties properties) {
        LOGGER.warning(() -> "Defining credentials individually with properties is deprecated."
                + " Provide a connection name instead in property " + CONNECTION_NAME_PROPERTY + ".");
        return getOracleConnectionDefinition(getOracleConnectionName(properties), properties.getUsername(),
                properties.getPassword());
    }

    protected String buildOracleConnectionDefinition(final AdapterProperties properties) {
        return "AT " + getOracleConnectionName(properties);
    }
}