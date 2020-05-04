package com.exasol.adapter.dialects.postgresql;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.BaseIdentifierConverter;
import com.exasol.adapter.jdbc.JdbcTypeDescription;
import com.exasol.adapter.metadata.DataType;

class PostgreSQLColumnMetadataReaderTest {
    private PostgreSQLColumnMetadataReader columnMetadataReader;
    private Map<String, String> rawProperties;

    @BeforeEach
    void beforeEach() {
        this.columnMetadataReader = createDefaultPostgreSQLColumnMetadataReader();
        this.rawProperties = new HashMap<>();
    }

    private PostgreSQLColumnMetadataReader createDefaultPostgreSQLColumnMetadataReader() {
        return new PostgreSQLColumnMetadataReader(null, AdapterProperties.emptyProperties(),
                BaseIdentifierConverter.createDefault());
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
                adapterProperties, BaseIdentifierConverter.createDefault());
        assertThat(columnMetadataReader.getIdentifierMapping(),
                equalTo(PostgreSQLIdentifierMapping.PRESERVE_ORIGINAL_CASE));
    }

    @Test
    void testGetConverToUpperPostgreSQLIdentifierMapping() {
        this.rawProperties.put("POSTGRESQL_IDENTIFIER_MAPPING", "CONVERT_TO_UPPER");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final PostgreSQLColumnMetadataReader columnMetadataReader = new PostgreSQLColumnMetadataReader(null,
                adapterProperties, BaseIdentifierConverter.createDefault());
        assertThat(columnMetadataReader.getIdentifierMapping(), equalTo(PostgreSQLIdentifierMapping.CONVERT_TO_UPPER));
    }
}
