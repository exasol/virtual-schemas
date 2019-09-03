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
import com.exasol.adapter.dialects.BaseIdentifierConverter;
import com.exasol.adapter.dialects.JdbcTypeDescription;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.metadata.DataType.ExaCharset;

class OracleColumnMetadataReaderTest {
    private OracleColumnMetadataReader columnMetadataReader;

    @BeforeEach
    void beforeEach() {
        this.columnMetadataReader = createDefaultOracleColumnMetadataReader();
    }

    protected OracleColumnMetadataReader createDefaultOracleColumnMetadataReader() {
        return new OracleColumnMetadataReader(null, AdapterProperties.emptyProperties(),
                BaseIdentifierConverter.createDefault());
    }

    @Test
    void testMapColumnTypeBeyondMaximumDecimalPrecision() {
        final int precision = DataType.MAX_EXASOL_DECIMAL_PRECISION + 1;
        final int scale = 0;
        final int castPrecision = 10;
        final int castScale = 2;
        final Map<String, String> rawProperties = new HashMap<>();
        rawProperties.put(OracleProperties.ORACLE_CAST_NUMBER_TO_DECIMAL_PROPERTY, castPrecision + "," + castScale);
        this.columnMetadataReader = createParameterizedColumnMetadataReader(rawProperties);
        final JdbcTypeDescription typeDescription = createTypeDescriptionForNumeric(precision, scale);
        assertThat(this.columnMetadataReader.mapJdbcType(typeDescription),
                equalTo(DataType.createDecimal(castPrecision, castScale)));
    }

    private JdbcTypeDescription createTypeDescriptionForNumeric(final int precision, final int scale) {
        final int octetLength = 10;
        return new JdbcTypeDescription(Types.NUMERIC, scale, precision, octetLength, "NUMERIC");
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

    @Test
    void testMapBlobMappedToUnsupportedTypeByDefault() {
        final JdbcTypeDescription typeDescription = new JdbcTypeDescription(Types.BLOB, 0, 0, 0, null);
        assertThat(this.columnMetadataReader.mapJdbcType(typeDescription), equalTo(DataType.createUnsupported()));
    }

    @Test
    void testMapBlobMappedToMaximumSizeVarCharIfBase64EncodingEnabled() {
        final Map<String, String> rawProperties = new HashMap<>();
        rawProperties.put("BINARY_COLUMN_HANDLING", "ENCODE_BASE64");
        final JdbcTypeDescription typeDescription = new JdbcTypeDescription(Types.BLOB, 0, 0, 0, null);
        assertThat(createParameterizedColumnMetadataReader(rawProperties).mapJdbcType(typeDescription),
                equalTo(DataType.createMaximumSizeVarChar(ExaCharset.UTF8)));
    }

    public OracleColumnMetadataReader createParameterizedColumnMetadataReader(final Map<String, String> rawProperties) {
        return new OracleColumnMetadataReader(null, new AdapterProperties(rawProperties),
                BaseIdentifierConverter.createDefault());
    }
}