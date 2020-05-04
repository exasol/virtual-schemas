package com.exasol.adapter.dialects.oracle;

import static com.exasol.adapter.dialects.oracle.OracleProperties.ORACLE_CONNECTION_NAME_PROPERTY;
import static com.exasol.adapter.dialects.oracle.OracleProperties.ORACLE_IMPORT_PROPERTY;

import com.exasol.ExaConnectionInformation;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.BaseConnectionDefinitionBuilder;

/**
 * This class implements an Oracle-specific connection definition builder.
 */
public class OracleConnectionDefinitionBuilder extends BaseConnectionDefinitionBuilder {
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
        if (properties.containsKey(ORACLE_CONNECTION_NAME_PROPERTY)) {
            return buildOracleConnectionDefinitionFromOracleConnectionOnly(properties);
        } else {
            throw new IllegalArgumentException("If you enable IMPORT FROM ORA with property \"" + ORACLE_IMPORT_PROPERTY
                    + "\" you also need to provide the name of an Oracle connection with \""
                    + ORACLE_CONNECTION_NAME_PROPERTY + "\".");
        }
    }

    private String getOracleConnectionName(final AdapterProperties properties) {
        return properties.get(ORACLE_CONNECTION_NAME_PROPERTY);
    }

    private String buildOracleConnectionDefinitionFromOracleConnectionOnly(final AdapterProperties properties) {
        return "AT " + getOracleConnectionName(properties);
    }
}