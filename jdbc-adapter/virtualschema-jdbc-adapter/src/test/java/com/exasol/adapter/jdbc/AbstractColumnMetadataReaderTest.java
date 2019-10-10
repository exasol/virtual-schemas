package com.exasol.adapter.jdbc;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.sql.Types;

import com.exasol.adapter.metadata.DataType;

public abstract class AbstractColumnMetadataReaderTest {
    protected ColumnMetadataReader columnMetadataReader;

    protected DataType mapJdbcType(final int type) {
        return mapJdbcTypeWithName(type, "");
    }

    protected DataType mapJdbcTypeWithName(final int type, final String typeName) {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(type, 0, 0, 0, typeName);
        final DataType jdbcType = this.columnMetadataReader.mapJdbcType(jdbcTypeDescription);
        return jdbcType;
    }

    protected void assertNumericMappedToDecimalWithPrecisionAndScale(final int expectedPrecision,
            final int expectedScale) {
        assertThat(mapNumeric(expectedScale, expectedPrecision),
                equalTo(DataType.createDecimal(expectedPrecision, expectedScale)));
    }

    protected DataType mapNumeric(final int expectedScale, final int expectedPrecision) {
        return this.columnMetadataReader
                .mapJdbcType(new JdbcTypeDescription(Types.NUMERIC, expectedScale, expectedPrecision, 0, ""));
    }

    protected void assertNumericMappedToDoubleWithPrecsionAndScale(final int expectedPrecision,
            final int expectedScale) {
        assertThat(mapNumeric(expectedScale, expectedPrecision), equalTo(DataType.createDouble()));
    }
}