package com.exasol.adapter.dialects.redshift;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.Types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.BaseIdentifierConverter;
import com.exasol.adapter.jdbc.AbstractColumnMetadataReaderTestBase;
import com.exasol.adapter.jdbc.JdbcTypeDescription;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.metadata.DataType.ExaCharset;

class RedshiftColumnMetadataReaderTest extends AbstractColumnMetadataReaderTestBase {
    @BeforeEach
    void beforeEach() {
        this.columnMetadataReader = new RedshiftColumnMetadataReader(null, AdapterProperties.emptyProperties(),
                BaseIdentifierConverter.createDefault());
    }

    @Test
    void testMapJdbcTypeNumeric() {
        assertNumericMappedToDecimalWithPrecisionAndScale(DataType.MAX_EXASOL_DECIMAL_PRECISION, 2);
    }

    @Test
    void testMapJdbcTypeNumericExceedingExsolMaxPrecisionToDouble() {
        assertNumericMappedToDoubleWithPrecsionAndScale(DataType.MAX_EXASOL_DECIMAL_PRECISION + 1, 2);
    }

    @Test
    void testMapJdbcTypeFallbackToParent() {
        assertThat(this.columnMetadataReader.mapJdbcType(new JdbcTypeDescription(Types.BOOLEAN, 0, 0, 0, "")),
                equalTo(DataType.createBool()));
    }

    @Test
    void testMapJdbcTypeOtherDouble() {
        assertThat(this.columnMetadataReader.mapJdbcType(new JdbcTypeDescription(Types.OTHER, 0, 0, 0, "double")),
                equalTo(DataType.createDouble()));
    }

    @Test
    void testMapJdbcTypeOtherUnknownToMaxVarChar() {
        assertThat(this.columnMetadataReader.mapJdbcType(new JdbcTypeDescription(Types.OTHER, 0, 0, 0, "unknown")),
                equalTo(DataType.createMaximumSizeVarChar(ExaCharset.UTF8)));
    }
}