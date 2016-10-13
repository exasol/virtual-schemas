package com.exasol.adapter.capabilities;

/**
 * This is an enumeration of the capabilities for literals supported by the EXASOL Virtual Schema Framework.
 *
 * <p>E.g. to execute a system "SELECT * FROM t WHERE username='alice'" your data source needs the {@link #STRING} capability.</p>
 */
public enum LiteralCapability {

    /**
     * The literal for null values.
     * <p>Example in EXASOL syntax: NULL</p>
     */
    NULL,

    /**
     * The literal for boolean values.
     * <p>Example in EXASOL syntax: TRUE</p>
     */
    BOOL,

    /**
     * The literal for date values.
     * <p>Example in EXASOL syntax: DATE '2000-01-28'</p>
     */
    DATE,

    /**
     * The literal for timestamp values.
     * <p>Example in EXASOL syntax: TIMESTAMP '2000-01-28 12:30:01.001'</p>
     */
    TIMESTAMP,

    /**
     * The literal for timestamp values. There is no direct literal for timestamps, but it can be created via casting.
     *
     * <p>Example in EXASOL syntax: CAST(TIMESTAMP '2000-01-28 12:30:01.001' AS TIMESTAMP WITH LOCAL TIME ZONE)</p>
     */
    TIMESTAMP_UTC,

    /**
     * The literal for double values.
     * <p>Example in EXASOL syntax: 100.23</p>
     */
    DOUBLE,

    /**
     * The literal for exact numeric values.
     * <p>Example in EXASOL syntax: 123</p>
     */
    EXACTNUMERIC,

    /**
     * The literal for string values.
     * <p>Example in EXASOL syntax: 'alice'</p>
     */
    STRING,

    /**
     * The literal for interval values.
     * <p>Example in EXASOL syntax: INTERVAL '5' MONTH</p>
     */
    INTERVAL
    
}
