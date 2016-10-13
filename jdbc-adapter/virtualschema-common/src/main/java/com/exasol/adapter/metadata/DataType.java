package com.exasol.adapter.metadata;


/**
 * Represents an EXASOL datatype.
 */
public class DataType {

    public static int maxExasolCharSize = 2000;
    public static int maxExasolVarcharSize = 2000000;
    public static int maxExasolDecimalPrecision = 36;

    private ExaDataType exaDataType;
    private int precision;
    private int scale;
    private int size;
    private ExaCharset charset;
    private boolean withLocalTimezone;
    private int geometrySrid;
    private IntervalType intervalType;
    private int intervalFraction;

    public enum ExaDataType {
        UNSUPPORTED,
        DECIMAL,
        DOUBLE,
        VARCHAR,
        CHAR,
        DATE,
        TIMESTAMP,
        BOOLEAN,
        GEOMETRY,
        INTERVAL
    }

    public enum ExaCharset {
        UTF8,
        ASCII
    }

    public enum IntervalType {
        DAY_TO_SECOND,
        YEAR_TO_MONTH
    }

    private DataType() {
    }

    public static DataType createVarChar(int size, ExaCharset charset) {
        DataType type = new DataType();
        type.exaDataType = ExaDataType.VARCHAR;
        type.size = size;
        type.charset = charset;
        return type;
    }

    public static DataType createChar(int size, ExaCharset charset) {
        DataType type = new DataType();
        type.exaDataType = ExaDataType.CHAR;
        type.size = size;
        type.charset = charset;
        return type;
    }

    public static DataType createDecimal(int precision, int scale) {
        DataType type = new DataType();
        type.exaDataType = ExaDataType.DECIMAL;
        type.precision = precision;
        type.scale = scale;
        return type;
    }

    public static DataType createDouble() {
        DataType type = new DataType();
        type.exaDataType = ExaDataType.DOUBLE;
        return type;
    }

    public static DataType createDate() {
        DataType type = new DataType();
        type.exaDataType = ExaDataType.DATE;
        return type;
    }

    public static DataType createTimestamp(boolean withLocalTimezone) {
        DataType type = new DataType();
        type.exaDataType = ExaDataType.TIMESTAMP;
        type.withLocalTimezone = withLocalTimezone;
        return type;
    }

    public static DataType createBool() {
        DataType type = new DataType();
        type.exaDataType = ExaDataType.BOOLEAN;
        return type;
    }

    public static DataType createGeometry(int srid) {
        DataType type = new DataType();
        type.exaDataType = ExaDataType.GEOMETRY;
        type.geometrySrid = srid;
        return type;
    }

    public static DataType createIntervalDaySecond(int precision, int fraction) {
        DataType type = new DataType();
        type.exaDataType = ExaDataType.INTERVAL;
        type.intervalType = IntervalType.DAY_TO_SECOND;
        type.precision = precision;
        type.intervalFraction = fraction;
        return type;
    }

    public static DataType createIntervalYearMonth(int precision) {
        DataType type = new DataType();
        type.exaDataType = ExaDataType.INTERVAL;
        type.intervalType = IntervalType.YEAR_TO_MONTH;
        type.precision = precision;
        return type;
    }

    public ExaDataType getExaDataType() {
        return exaDataType;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public int getSize() {
        return size;
    }

    public ExaCharset getCharset() {
        return charset;
    }

    public boolean isWithLocalTimezone() {
        return withLocalTimezone;
    }

    public int getGeometrySrid() {
        return geometrySrid;
    }

    public IntervalType getIntervalType() {
        return intervalType;
    }

    public int getIntervalFraction() {
        return intervalFraction;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        switch (exaDataType) {
            case UNSUPPORTED:
                builder.append("UNSUPPORTED");
                break;
            case DECIMAL:
                builder.append("DECIMAL(");
                builder.append(precision);
                builder.append(", ");
                builder.append(scale);
                builder.append(")");
                break;
            case DOUBLE:
                builder.append("DOUBLE");
                break;
            case VARCHAR:
                builder.append("VARCHAR(");
                builder.append(size);
                builder.append(") ");
                builder.append(charset.toString());
                break;
            case CHAR:
                builder.append("CHAR(");
                builder.append(size);
                builder.append(") ");
                builder.append(charset.toString());
                break;
            case DATE:
                builder.append("DATE");
                break;
            case TIMESTAMP:
                builder.append("TIMESTAMP");
                if (withLocalTimezone) {
                    builder.append(" WITH LOCAL TIME ZONE");
                }
                break;
            case BOOLEAN:
                builder.append("BOOLEAN");
                break;
            case GEOMETRY:
                builder.append("GEOMETRY");
                builder.append("(");
                builder.append(geometrySrid);
                builder.append(")");
                break;
            case INTERVAL:
                builder.append("INTERVAL ");
                if (intervalType == IntervalType.YEAR_TO_MONTH) {
                    builder.append("YEAR");
                    builder.append(" (");
                    builder.append(precision);
                    builder.append(")");
                    builder.append(" TO MONTH");
                } else {
                    builder.append("DAY");
                    builder.append(" (");
                    builder.append(precision);
                    builder.append(")");
                    builder.append(" TO SECOND");
                    builder.append(" (");
                    builder.append(intervalFraction);
                    builder.append(")");
                }
                break;
        }
        return builder.toString();
    }
}
