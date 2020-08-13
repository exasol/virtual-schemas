package com.exasol.adapter.dialects.sqlserver;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.sql.Types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.BaseIdentifierConverter;
import com.exasol.adapter.jdbc.AbstractColumnMetadataReaderTestBase;
import com.exasol.adapter.jdbc.JdbcTypeDescription;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.metadata.DataType.ExaCharset;

class SqlServerColumnMetadataReaderTest extends AbstractColumnMetadataReaderTestBase {
    @BeforeEach
    void beforeEach() {
        this.columnMetadataReader = new SqlServerColumnMetadataReader(null, AdapterProperties.emptyProperties(),
                BaseIdentifierConverter.createDefault());
    }

    @Test
    void testMapJdbcTypeVarChar() {
        final int expectedSize = 42;
        final DataType type = this.columnMetadataReader
                .mapJdbcType(new JdbcTypeDescription(Types.VARCHAR, 0, expectedSize, expectedSize, ""));
        assertThat(type, equalTo(DataType.createVarChar(expectedSize, ExaCharset.ASCII)));
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
        final DataType jdbcType = mapJdbcType(Types.BOOLEAN);
        assertThat(jdbcType, equalTo(DataType.createBool()));
    }
}