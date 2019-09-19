package com.exasol.adapter.dialects.mysql;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.BaseIdentifierConverter;
import com.exasol.adapter.dialects.JdbcTypeDescription;
import com.exasol.adapter.dialects.hive.HiveColumnMetadataReader;
import com.exasol.adapter.metadata.DataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Types;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

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