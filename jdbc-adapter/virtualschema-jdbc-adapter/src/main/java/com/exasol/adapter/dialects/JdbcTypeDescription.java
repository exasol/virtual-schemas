package com.exasol.adapter.dialects;

public class JdbcTypeDescription {
    private final int jdbcType;
    private final int decimalScale;
    private final int precisionOrSize;
    private final int charOctedLength;
    private final String typeName;

    public JdbcTypeDescription(int jdbcType, int decimalScale, int precisionOrSize,
                               int charOctedLength
    ) {
        this(jdbcType, decimalScale, precisionOrSize, charOctedLength, null);
    }

    public JdbcTypeDescription(int jdbcType, int decimalScale, int precisionOrSize, int charOctedLength,
                               String typeName) {
        this.jdbcType = jdbcType;
        this.decimalScale = decimalScale;
        this.precisionOrSize = precisionOrSize;
        this.charOctedLength = charOctedLength;
        this.typeName = typeName;
    }

    public int getJdbcType() {
        return jdbcType;
    }

    public int getDecimalScale() {
        return decimalScale;
    }

    public int getPrecisionOrSize() {
        return precisionOrSize;
    }

    public int getCharOctedLength() {
        return charOctedLength;
    }

    public String getTypeName() {
        return typeName;
    }
}
