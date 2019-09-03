package com.exasol.adapter.dialects.oracle;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.*;

import org.junit.jupiter.api.*;

import com.exasol.adapter.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.metadata.*;
import com.exasol.adapter.metadata.DataType.*;

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

    private OracleColumnMetadataReader createParameterizedColumnMetadataReader(
            final Map<String, String> rawProperties) {
        return new OracleColumnMetadataReader(null, new AdapterProperties(rawProperties),
                BaseIdentifierConverter.createDefault());
    }
}