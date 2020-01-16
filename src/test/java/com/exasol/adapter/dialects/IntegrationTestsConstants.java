package com.exasol.adapter.dialects;

import java.nio.file.Path;

public final class IntegrationTestsConstants {
    public static final String VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION = "virtualschema-jdbc-adapter-dist-3.0.1.jar";
    public static final Path PATH_TO_VIRTUAL_SCHEMAS_JAR = Path.of("target/" + VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);
    public static final String SCHEMA_EXASOL_TEST = "SCHEMA_EXASOL_TEST";
}
