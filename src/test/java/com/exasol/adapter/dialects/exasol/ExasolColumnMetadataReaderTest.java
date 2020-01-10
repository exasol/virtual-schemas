package com.exasol.adapter.dialects.exasol;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.Types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.BaseIdentifierConverter;
import com.exasol.adapter.jdbc.JdbcTypeDescription;
import com.exasol.adapter.metadata.DataType;

class ExasolColumnMetadataReaderTest {
    private ExasolColumnMetadataReader exasolColumnMetadataReader;

    @BeforeEach
    void beforeEach() {
        this.exasolColumnMetadataReader = new ExasolColumnMetadataReader(null, AdapterProperties.emptyProperties(),
                BaseIdentifierConverter.createDefault());
    }

    @Test
    void testMapJdbcTypeIntervalDayToSeconds() {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(
                ExasolColumnMetadataReader.EXASOL_INTERVAL_DAY_TO_SECONDS, 0, 0, 0, "INTERVAL_DAY_TO_SECONDS");
        assertThat(this.exasolColumnMetadataReader.mapJdbcType(jdbcTypeDescription),
                equalTo(DataType.createIntervalDaySecond(2, 3)));
    }

    @Test
    void testMapJdbcTypeIntervalYearToMonths() {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(
                ExasolColumnMetadataReader.EXASOL_INTERVAL_YEAR_TO_MONTHS, 0, 0, 0, "INTERVAL_YEAR_TO_MONTHS");
        assertThat(this.exasolColumnMetadataReader.mapJdbcType(jdbcTypeDescription),
                equalTo(DataType.createIntervalYearMonth(2)));
    }

    @Test
    void testMapJdbcTypeGeometry() {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(
                ExasolColumnMetadataReader.EXASOL_GEOMETRY, 0, 0, 0, "GEOMETRY");
        assertThat(this.exasolColumnMetadataReader.mapJdbcType(jdbcTypeDescription),
                equalTo(DataType.createGeometry(3857)));
    }

    @Test
    void testMapJdbcTypeTimestamp() {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(
                ExasolColumnMetadataReader.EXASOL_TIMESTAMP, 0, 0, 0, "TIMESTAMP");
        assertThat(this.exasolColumnMetadataReader.mapJdbcType(jdbcTypeDescription),
                equalTo(DataType.createTimestamp(true)));
    }

    @Test
    void testMapJdbcTypeHashtype() {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(
              ExasolColumnMetadataReader.EXASOL_HASHTYPE, 0, 0, 16, "HASHTYPE");
        assertThat(this.exasolColumnMetadataReader.mapJdbcType(jdbcTypeDescription),
              equalTo(DataType.createHashtype(16)));
    }

    @Test
    void testMapJdbcTypeDefault() {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(Types.BOOLEAN, 0, 0, 0, "BOOLEAN");
        assertThat(this.exasolColumnMetadataReader.mapJdbcType(jdbcTypeDescription), equalTo(DataType.createBool()));
    }
}