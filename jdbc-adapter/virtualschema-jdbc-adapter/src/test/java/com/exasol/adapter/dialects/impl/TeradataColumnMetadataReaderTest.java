package com.exasol.adapter.dialects.impl;

import static com.exasol.adapter.dialects.impl.TeradataColumnMetadataReader.MAX_TERADATA_VARCHAR_SIZE;
import static com.exasol.adapter.metadata.DataType.MAX_EXASOL_DECIMAL_PRECISION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.sql.Types;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.JdbcTypeDescription;
import com.exasol.adapter.metadata.DataType;

class TeradataColumnMetadataReaderTest {
    private TeradataColumnMetadataReader teradataColumnMetadataReader;

    @BeforeEach
    void beforeEach() {
        this.teradataColumnMetadataReader = new TeradataColumnMetadataReader(null, AdapterProperties.emptyProperties());
    }

    @Test
    void testMapJdbcTypeTime() {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(Types.TIME, 0, 0, 0, "");
        assertThat(this.teradataColumnMetadataReader.mapJdbcType(jdbcTypeDescription),
                equalTo(DataType.createVarChar(21, DataType.ExaCharset.UTF8)));
    }

    @Test
    void testMapJdbcTypeTimeWithTimezone() {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(Types.TIME_WITH_TIMEZONE, 0, 0, 0, "");
        assertThat(this.teradataColumnMetadataReader.mapJdbcType(jdbcTypeDescription),
                equalTo(DataType.createVarChar(21, DataType.ExaCharset.UTF8)));
    }

    @Test
    void testMapJdbcTypeNumericWithDecimalPrecisionLessThanMax() {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(Types.NUMERIC, 5,
                MAX_EXASOL_DECIMAL_PRECISION, 0, "");
        assertThat(this.teradataColumnMetadataReader.mapJdbcType(jdbcTypeDescription),
                equalTo(DataType.createDecimal(MAX_EXASOL_DECIMAL_PRECISION, 5)));
    }

    @Test
    void testMapJdbcTypeNumericWithDecimalPrecisionHigherThanMax() {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(Types.NUMERIC, 0,
                MAX_EXASOL_DECIMAL_PRECISION + 1, 0, "");
        assertThat(this.teradataColumnMetadataReader.mapJdbcType(jdbcTypeDescription),
                equalTo(DataType.createDouble()));
    }

    @Test
    void testMapJdbcTypeGeometry() {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(Types.OTHER, 0, 5, 0, "GEOMETRY");
        assertThat(this.teradataColumnMetadataReader.mapJdbcType(jdbcTypeDescription),
                equalTo(DataType.createVarChar(5, DataType.ExaCharset.UTF8)));
    }

    @Test
    void testMapJdbcTypeInterval() {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(Types.OTHER, 0, 5, 0, "INTERVAL");
        assertThat(this.teradataColumnMetadataReader.mapJdbcType(jdbcTypeDescription),
                equalTo(DataType.createVarChar(30, DataType.ExaCharset.UTF8)));
    }

    @Test
    void testMapJdbcTypePeriod() {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(Types.OTHER, 0, 5, 0, "PERIOD");
        assertThat(this.teradataColumnMetadataReader.mapJdbcType(jdbcTypeDescription),
                equalTo(DataType.createVarChar(100, DataType.ExaCharset.UTF8)));
    }

    @Test
    void testMapJdbcTypeOther() {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(Types.OTHER, 0, 5, 0, "");
        assertThat(this.teradataColumnMetadataReader.mapJdbcType(jdbcTypeDescription),
                equalTo(DataType.createVarChar(MAX_TERADATA_VARCHAR_SIZE, DataType.ExaCharset.UTF8)));
    }

    @Test
    void testMapJdbcTypeSqlxml() {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(Types.SQLXML, 0, 0, 0, "");
        assertThat(this.teradataColumnMetadataReader.mapJdbcType(jdbcTypeDescription),
                equalTo(DataType.createVarChar(MAX_TERADATA_VARCHAR_SIZE, DataType.ExaCharset.UTF8)));
    }

    @Test
    void testMapJdbcTypeClob() {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(Types.CLOB, 0, 0, 0, "");
        assertThat(this.teradataColumnMetadataReader.mapJdbcType(jdbcTypeDescription),
                equalTo(DataType.createVarChar(MAX_TERADATA_VARCHAR_SIZE, DataType.ExaCharset.UTF8)));
    }

    @Test
    void testMapJdbcTypeBlob() {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(Types.BLOB, 0, 0, 0, "");
        assertThat(this.teradataColumnMetadataReader.mapJdbcType(jdbcTypeDescription),
                equalTo(DataType.createVarChar(100, DataType.ExaCharset.UTF8)));
    }

    @Test
    void testMapJdbcTypeVarbinary() {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(Types.VARBINARY, 0, 0, 0, "");
        assertThat(this.teradataColumnMetadataReader.mapJdbcType(jdbcTypeDescription),
                equalTo(DataType.createVarChar(100, DataType.ExaCharset.UTF8)));
    }

    @Test
    void testMapJdbcTypeBinary() {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(Types.BINARY, 0, 0, 0, "");
        assertThat(this.teradataColumnMetadataReader.mapJdbcType(jdbcTypeDescription),
                equalTo(DataType.createVarChar(100, DataType.ExaCharset.UTF8)));
    }

    @Test
    void testMapJdbcTypeDistinct() {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(Types.DISTINCT, 0, 0, 0, "");
        assertThat(this.teradataColumnMetadataReader.mapJdbcType(jdbcTypeDescription),
                equalTo(DataType.createVarChar(100, DataType.ExaCharset.UTF8)));
    }

    @Test
    void testMapJdbcTypeDefault() {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(Types.BOOLEAN, 0, 0, 0, "BOOLEAN");
        assertThat(this.teradataColumnMetadataReader.mapJdbcType(jdbcTypeDescription),
                CoreMatchers.equalTo(DataType.createBool()));
    }
}