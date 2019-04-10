package com.exasol.adapter.dialects.impl;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.sql.Types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.JdbcTypeDescription;
import com.exasol.adapter.metadata.DataType;

class PostgreSQLColumnMetadataReaderTest {
    private PostgreSQLColumnMetadataReader columnMetadataReader;

    @BeforeEach
    void beforeEach() {
        this.columnMetadataReader = new PostgreSQLColumnMetadataReader(null, AdapterProperties.emptyProperties());
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
}
