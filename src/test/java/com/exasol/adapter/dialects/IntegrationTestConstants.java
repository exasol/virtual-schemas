package com.exasol.adapter.dialects;

import java.nio.file.Path;

public final class IntegrationTestConstants {
    public static final String VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION = "virtual-schema-dist-7.0.0-bundle-4.0.4.jar";
    public static final String EXASOL_DOCKER_IMAGE_REFERENCE = "exasol/docker-db:6.2.9-d1";
    public static final Path PATH_TO_VIRTUAL_SCHEMAS_JAR = Path.of("target", VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);
    public static final String SCHEMA_EXASOL = "SCHEMA_EXASOL";
    public static final String ADAPTER_SCRIPT_EXASOL = "ADAPTER_SCRIPT_EXASOL";
    public static final String TABLE_JOIN_1 = "TABLE_JOIN_1";
    public static final String TABLE_JOIN_2 = "TABLE_JOIN_2";
    public static final String DOCKER_IP_ADDRESS = "172.17.0.1";
    public static final String JDBC_DRIVER_CONFIGURATION_FILE_NAME = "settings.cfg";

    private IntegrationTestConstants() {
        // intentionally left empty
    }
}
