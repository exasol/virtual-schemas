package com.exasol.adapter.dialects;

import java.nio.file.Path;

public final class IntegrationTestsConstants {
    public static final String VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION = "virtualschema-jdbc-adapter-dist-3.0.1.jar";
    public static final Path PATH_TO_VIRTUAL_SCHEMAS_JAR = Path.of("target", VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);
    public static final String SCHEMA_EXASOL_TEST = "SCHEMA_EXASOL_TEST";
    public static final String ADAPTER_SCRIPT_EXASOL = "ADAPTER_SCRIPT_EXASOL";
    public static final String TABLE_JOIN_1 = "TABLE_JOIN_1";
    public static final String TABLE_JOIN_2 = "TABLE_JOIN_2";

    public static final String POSTGRES_DRIVER_NAME_AND_VERSION = "postgresql-42.2.5.jar";
    public static final Path PATH_TO_POSTGRES_DRIVER = Path.of("src", "test", "resources", "integration", "driver",
            POSTGRES_DRIVER_NAME_AND_VERSION);
    public static final String POSTGRES_DOCKER_VERSION = "postgres:9.6.2";

    private IntegrationTestsConstants() {
        // intentionally left empty
    }
}
