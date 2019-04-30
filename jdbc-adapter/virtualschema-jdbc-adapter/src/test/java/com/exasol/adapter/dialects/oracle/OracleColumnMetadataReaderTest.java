package com.exasol.adapter.dialects.oracle;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.JdbcTypeDescription;
import com.exasol.adapter.metadata.DataType;

class OracleColumnMetadataReaderTest {
    private OracleColumnMetadataReader columnMetadataReader;

    @BeforeEach
    void beforeEach() {
        this.columnMetadataReader = new OracleColumnMetadataReader(null, AdapterProperties.emptyProperties());
    }

    @ValueSource(strings = { "10,2", " 10,2", " 10 , 2 " })
    @ParameterizedTest
    void testMapColumnTypeBeyondMaximumDecimalPrecision(final String input) {
        final int precision = DataType.MAX_EXASOL_DECIMAL_PRECISION + 1;
        final int scale = 0;
        final int castPrecision = 10;
        final int castScale = 2;
        final Map<String, String> rawProperties = new HashMap<>();
        rawProperties.put(OracleSqlDialect.ORACLE_CAST_NUMBER_TO_DECIMAL_PROPERTY, castPrecision + "," + castScale);
        this.columnMetadataReader = new OracleColumnMetadataReader(null, new AdapterProperties(rawProperties));
        final JdbcTypeDescription typeDescription = createTypeDescriptionForNumeric(precision, scale);
        assertThat(this.columnMetadataReader.mapJdbcType(typeDescription),
                equalTo(DataType.createDecimal(castPrecision, castScale)));
    }

    private JdbcTypeDescription createTypeDescriptionForNumeric(final int precision, final int scale) {
        final int octetLength = 10;
        return new JdbcTypeDescription(Types.NUMERIC, scale, precision,
                octetLength, "NUMERIC");
    }

    @Test
    void testMapColumnTypeWithMagicScale() {
        final int precision = 10;
        final int scale = OracleColumnMetadataReader.ORACLE_MAGIC_NUMBER_SCALE;
        final JdbcTypeDescription typeDescription = createTypeDescriptionForNumeric(precision, scale);
        assertThat(this.columnMetadataReader.mapJdbcType(typeDescription),
                equalTo(DataType.createMaximumSizeVarChar(DataType.ExaCharset.UTF8)));
    }

    @Test
    void testMapColumnTypeWithMaximumDecimalPrecision() {
        final int precision = DataType.MAX_EXASOL_DECIMAL_PRECISION;
        final int scale = 0;
        final JdbcTypeDescription typeDescription = createTypeDescriptionForNumeric(precision, scale);
        assertThat(this.columnMetadataReader.mapJdbcType(typeDescription),
                equalTo(DataType.createDecimal(precision, scale)));
    }
}
