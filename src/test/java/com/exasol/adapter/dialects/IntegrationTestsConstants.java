package com.exasol.adapter.dialects;

import java.nio.file.Path;

public final class IntegrationTestsConstants {
    public static final String VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION = "virtualschema-jdbc-adapter-dist-3.0.1.jar";
    public static final Path PATH_TO_VIRTUAL_SCHEMAS_JAR = Path.of("target", VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);
    public static final String SCHEMA_EXASOL = "SCHEMA_EXASOL";
    public static final String ADAPTER_SCRIPT_EXASOL = "ADAPTER_SCRIPT_EXASOL";
    public static final String TABLE_JOIN_1 = "TABLE_JOIN_1";
    public static final String TABLE_JOIN_2 = "TABLE_JOIN_2";
    public static final String DOCKER_IP_ADDRESS = "172.17.0.1";

    public static final String POSTGRES_DRIVER_NAME_AND_VERSION = "postgresql-42.2.5.jar";
    public static final Path PATH_TO_POSTGRES_DRIVER = Path.of("src", "test", "resources", "integration", "driver",
            "postgres", POSTGRES_DRIVER_NAME_AND_VERSION);
    public static final String POSTGRES_DOCKER_VERSION = "postgres:9.6.2";

    public static final String ORACLE_DOCKER_VERSION = "oracleinanutshell/oracle-xe-11g";
    public static final String JDBC_DRIVER_CONFIGURATION_FILE_NAME = "settings.cfg";
    public static final Path ORACLE_DRIVER_SETTINGS_PATH = Path.of("src", "test", "resources", "integration", "driver",
            "oracle", JDBC_DRIVER_CONFIGURATION_FILE_NAME);

    private IntegrationTestsConstants() {
        // intentionally left empty
    }
}
