package com.exasol.adapter.dialects.hive;

import static com.exasol.adapter.dialects.hive.HiveProperties.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.*;

import org.junit.jupiter.api.*;

import com.exasol.adapter.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.*;
import com.exasol.adapter.metadata.*;

class HiveColumnMetadataReaderTest {
    private HiveColumnMetadataReader columnMetadataReader;

    @BeforeEach
    void beforeEach() {
        this.columnMetadataReader = new HiveColumnMetadataReader(null, AdapterProperties.emptyProperties(),
                BaseIdentifierConverter.createDefault());
    }

    @Test
    void testMapColumnTypeWithMaxExasolDecimalPrecision() {
        final int precision = DataType.MAX_EXASOL_DECIMAL_PRECISION;
        final int scale = 0;
        final JdbcTypeDescription typeDescription = new JdbcTypeDescription(Types.DECIMAL, scale, precision, 10,
                "DECIMAL");
        assertThat(this.columnMetadataReader.mapJdbcType(typeDescription),
                equalTo(DataType.createDecimal(precision, scale)));
    }

    @Test
    void testMapColumnTypeBeyondMaxExasolDecimalPrecision() {
        final JdbcTypeDescription typeDescription = new JdbcTypeDescription(Types.DECIMAL, 0,
                DataType.MAX_EXASOL_DECIMAL_PRECISION + 1, 10, "DECIMAL");
        assertThat(this.columnMetadataReader.mapJdbcType(typeDescription),
                equalTo(DataType.createMaximumSizeVarChar(DataType.ExaCharset.UTF8)));
    }

    @Test
    void testMapColumnTypeBeyondMaxExasolDecimalPrecisionWithCastProperty() {
        final Map<String, String> rawProperties = new HashMap<>();
        rawProperties.put(HIVE_CAST_NUMBER_TO_DECIMAL_PROPERTY, "10,2");
        final AdapterProperties properties = new AdapterProperties(rawProperties);
        final ColumnMetadataReader columnMetadataReader = new HiveColumnMetadataReader(null, properties,
                BaseIdentifierConverter.createDefault());
        final JdbcTypeDescription typeDescription = new JdbcTypeDescription(Types.DECIMAL, 0,
                DataType.MAX_EXASOL_DECIMAL_PRECISION + 1, 10, "DECIMAL");
        assertThat(columnMetadataReader.mapJdbcType(typeDescription), equalTo(DataType.createDecimal(10, 2)));
    }
}