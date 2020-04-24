package com.exasol.adapter.dialects.hive;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.BaseIdentifierConverter;
import com.exasol.adapter.jdbc.ColumnMetadataReader;
import com.exasol.adapter.jdbc.JdbcTypeDescription;
import com.exasol.adapter.metadata.DataType;

class HiveColumnMetadataReaderTest {
    private HiveColumnMetadataReader columnMetadataReader;

    @BeforeEach
    void beforeEach() {
        this.columnMetadataReader = new HiveColumnMetadataReader(null, AdapterProperties.emptyProperties(),
                BaseIdentifierConverter.createDefault());
    }

    @Test
    void mapDecimalReturnDecimal() {
        final JdbcTypeDescription typeDescription = new JdbcTypeDescription(Types.DECIMAL, 0,
                DataType.MAX_EXASOL_DECIMAL_PRECISION, 10, "DECIMAL");
        assertThat(this.columnMetadataReader.mapJdbcType(typeDescription), equalTo(DataType.createDecimal(36, 0)));
    }

    @Test
    void mapDecimalReturnVarchar() {
        final JdbcTypeDescription typeDescription = new JdbcTypeDescription(Types.DECIMAL, 0,
                DataType.MAX_EXASOL_DECIMAL_PRECISION + 1, 10, "DECIMAL");
        assertThat(this.columnMetadataReader.mapJdbcType(typeDescription),
                equalTo(DataType.createMaximumSizeVarChar(DataType.ExaCharset.UTF8)));
    }

    @Test
    void testMapColumnTypeBeyondMaxExasolDecimalPrecisionWithCastProperty() {
        final Map<String, String> rawProperties = new HashMap<>();
        rawProperties.put(HiveProperties.HIVE_CAST_NUMBER_TO_DECIMAL_PROPERTY, "10,2");
        final AdapterProperties properties = new AdapterProperties(rawProperties);
        final ColumnMetadataReader columnMetadataReader = new HiveColumnMetadataReader(null, properties,
                BaseIdentifierConverter.createDefault());
        final JdbcTypeDescription typeDescription = new JdbcTypeDescription(Types.DECIMAL, 0,
                DataType.MAX_EXASOL_DECIMAL_PRECISION + 1, 10, "DECIMAL");
        assertThat(columnMetadataReader.mapJdbcType(typeDescription), equalTo(DataType.createDecimal(10, 2)));
    }
}