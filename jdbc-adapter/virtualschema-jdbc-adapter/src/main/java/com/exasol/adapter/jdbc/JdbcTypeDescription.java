package com.exasol.adapter.jdbc;

/**
 * This class implements a parameter object describing attributes of a JDBC data type.
 * <p>
 * The size of a type can differ from the octet length of that type if the data type uses multiple bytes to represent a
 * character. This is for example the case for UTF encoding.
 * <p>
 * The original name helps distinguishing types that have the same identifier number but different meaning.
 */
public class JdbcTypeDescription {
    private final int jdbcType;
    private final int decimalScale;
    private final int precisionOrSize;
    private final int byteSize;
    private final String typeName;

    /**
     * Create a new instance of a JDBC data type.
     *
     * @param jdbcType        type identifier as presented by the JDBC driver of the database
     * @param decimalScale    number of digits behind the point
     * @param precisionOrSize precision for numbers or size for size types (like <code>VARCHAR</code>)
     * @param byteSize        storage size the data type needs in bytes
     * @param typeName        original name the type has in the database
     */
    public JdbcTypeDescription(final int jdbcType, final int decimalScale, final int precisionOrSize,
            final int byteSize, final String typeName) {
        this.jdbcType = jdbcType;
        this.decimalScale = decimalScale;
        this.precisionOrSize = precisionOrSize;
        this.byteSize = byteSize;
        this.typeName = typeName;
    }

    /**
     * Get the JDBC type identifier.
     *
     * @return type identifier as number
     */
    public int getJdbcType() {
        return this.jdbcType;
    }

    /**
     * Get the decimal scale.
     *
     * @return decimal scale
     */
    public int getDecimalScale() {
        return this.decimalScale;
    }

    /**
     * Get the precision for numeric types or size for size types (like <code>VARCHAR</code>).
     *
     * @return precision or size
     */
    public int getPrecisionOrSize() {
        return this.precisionOrSize;
    }

    /**
     * Storage size in eight-bit bytes (aka. octets).
     *
     * @return size in bytes
     */
    public int getByteSize() {
        return this.byteSize;
    }

    /**
     * Get the original name of the data type.
     *
     * @return original type name
     */
    public String getTypeName() {
        return this.typeName;
    }
}