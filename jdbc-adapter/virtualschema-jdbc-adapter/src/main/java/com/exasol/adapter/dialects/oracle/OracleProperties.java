package com.exasol.adapter.dialects.oracle;

/**
 * This class contains Oracle-specific adapter properties
 */
public final class OracleProperties {
    public static final String ORACLE_CAST_NUMBER_TO_DECIMAL_PROPERTY = "ORACLE_CAST_NUMBER_TO_DECIMAL_WITH_PRECISION_AND_SCALE";
    public static final String ORACLE_IMPORT_PROPERTY = "IMPORT_FROM_ORA";
    public static final String ORACLE_CONNECTION_NAME_PROPERTY = "ORA_CONNECTION_NAME";

    private OracleProperties() {
        // prevent instantiation
    }
}