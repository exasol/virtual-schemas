package com.exasol.adapter.dialects.mysql;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.Types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.BaseIdentifierConverter;
import com.exasol.adapter.dialects.JdbcTypeDescription;
import com.exasol.adapter.metadata.DataType;

class MySqlColumnMetadataReaderTest {
    private MySqlColumnMetadataReader columnMetadataReader;

    @BeforeEach
    void beforeEach() {
        this.columnMetadataReader = new MySqlColumnMetadataReader(null, AdapterProperties.emptyProperties(),
                BaseIdentifierConverter.createDefault());
    }

    @Test
    void mapTime() {
        final JdbcTypeDescription typeDescription = new JdbcTypeDescription(Types.TIME, 0, 0, 10, "TIME");
        assertThat(columnMetadataReader.mapJdbcType(typeDescription), equalTo(DataType.createTimestamp(false)));
    }

    @Test
    void mapBinary() {
        final JdbcTypeDescription typeDescription = new JdbcTypeDescription(Types.BINARY, 0, 0, 10, "BINARY");
        assertThat(columnMetadataReader.mapJdbcType(typeDescription), equalTo(DataType.createUnsupported()));
    }
}