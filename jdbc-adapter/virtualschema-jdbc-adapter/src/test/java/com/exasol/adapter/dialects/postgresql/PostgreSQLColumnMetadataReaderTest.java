package com.exasol.adapter.dialects.postgresql;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.jdbc.ColumnMetadataReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.JdbcTypeDescription;
import com.exasol.adapter.metadata.DataType;

class PostgreSQLColumnMetadataReaderTest {
    private PostgreSQLColumnMetadataReader columnMetadataReader;
    private Map<String, String> rawProperties;

    @BeforeEach
    void beforeEach() {
        this.columnMetadataReader = new PostgreSQLColumnMetadataReader(null, AdapterProperties.emptyProperties());
        rawProperties = new HashMap<>();
    }

    @Test
    void testMapJdbcTypeOther() {
        assertThat(mapJdbcType(Types.OTHER), equalTo(DataType.createMaximumSizeVarChar(DataType.ExaCharset.UTF8)));
    }

    protected DataType mapJdbcType(final int type) {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(type, 0, 0, 0, "");
        return this.columnMetadataReader.mapJdbcType(jdbcTypeDescription);
    }

    @ValueSource(ints = { Types.SQLXML, Types.DISTINCT })
    @ParameterizedTest
    void testMapJdbcTypeFallbackToMaxVarChar(final int type) {
        assertThat(mapJdbcType(type), equalTo(DataType.createMaximumSizeVarChar(DataType.ExaCharset.UTF8)));
    }

    @Test
    void testMapJdbcTypeFallbackToParent() {
        assertThat(mapJdbcType(Types.BOOLEAN), equalTo(DataType.createBool()));
    }

    @Test
    void testGetDefaultPostgreSQLIdentifierMapping() {
        assertThat(this.columnMetadataReader.getIdentifierMapping(),
                equalTo(PostgreSQLIdentifierMapping.CONVERT_TO_UPPER));
    }

    @Test
    void testGetPreserveCasePostgreSQLIdentifierMapping() {
        this.rawProperties.put("POSTGRESQL_IDENTIFIER_MAPPING", "PRESERVE_ORIGINAL_CASE");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final PostgreSQLColumnMetadataReader columnMetadataReader = new PostgreSQLColumnMetadataReader(null,
                adapterProperties);
        assertThat(columnMetadataReader.getIdentifierMapping(),
                equalTo(PostgreSQLIdentifierMapping.PRESERVE_ORIGINAL_CASE));
    }

    @Test
    void testGetConverToUpperPostgreSQLIdentifierMapping() {
        this.rawProperties.put("POSTGRESQL_IDENTIFIER_MAPPING", "CONVERT_TO_UPPER");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final PostgreSQLColumnMetadataReader columnMetadataReader = new PostgreSQLColumnMetadataReader(null,
                adapterProperties);
        assertThat(columnMetadataReader.getIdentifierMapping(), equalTo(PostgreSQLIdentifierMapping.CONVERT_TO_UPPER));
    }
}
