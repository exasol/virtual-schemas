package com.exasol.adapter.dialects.oracle;

import static com.exasol.adapter.AdapterProperties.CONNECTION_NAME_PROPERTY;
import static com.exasol.adapter.dialects.oracle.OracleProperties.ORACLE_CONNECTION_NAME_PROPERTY;
import static com.exasol.adapter.dialects.oracle.OracleProperties.ORACLE_IMPORT_PROPERTY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;

class OracleConnectionDefinitionBuilderTest {
    private final OracleConnectionDefinitionBuilder connectionDefinitionBuilder = new OracleConnectionDefinitionBuilder();

    @Test
    void testBuildConnectionDefinition() {
        final AdapterProperties properties = new AdapterProperties(Map.of( //
                ORACLE_IMPORT_PROPERTY, "true", //
                ORACLE_CONNECTION_NAME_PROPERTY, "ora_connection", //
                CONNECTION_NAME_PROPERTY, "jdbc_connection"));
        assertThat(connectionDefinitionBuilder.buildConnectionDefinition(properties, null),
                containsString("AT ora_connection"));
    }

    @Test
    void testBuildConnectionDefinitionMissingPropertyException() {
        final AdapterProperties properties = new AdapterProperties(Map.of( //
                ORACLE_IMPORT_PROPERTY, "true", //
                CONNECTION_NAME_PROPERTY, "ora_connection"));
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> connectionDefinitionBuilder.buildConnectionDefinition(properties, null));
        assertThat(exception.getMessage(), containsString("If you enable IMPORT FROM ORA with property"));
    }
}