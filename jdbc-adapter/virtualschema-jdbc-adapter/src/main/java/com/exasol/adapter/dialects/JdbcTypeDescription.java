package com.exasol.adapter.dialects;

public class JdbcTypeDescription {
    private final int jdbcType;
    private final int decimalScale;
    private final int precisionOrSize;
    private final int charOctedLength;
    private final String typeName;

    public JdbcTypeDescription(final int jdbcType, final int decimalScale, final int precisionOrSize,
            final int charOctedLength, final String typeName) {
        this.jdbcType = jdbcType;
        this.decimalScale = decimalScale;
        this.precisionOrSize = precisionOrSize;
        this.charOctedLength = charOctedLength;
        this.typeName = typeName;
    }

    public int getJdbcType() {
        return this.jdbcType;
    }

    public int getDecimalScale() {
        return this.decimalScale;
    }

    public int getPrecisionOrSize() {
        return this.precisionOrSize;
    }

    public int getCharOctedLength() {
        return this.charOctedLength;
    }

    public String getTypeName() {
        return this.typeName;
    }
}
