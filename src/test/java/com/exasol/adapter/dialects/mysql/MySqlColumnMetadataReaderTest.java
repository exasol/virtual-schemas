package com.exasol.adapter.dialects.mysql;

import static com.exasol.adapter.dialects.mysql.MySqlColumnMetadataReader.TEXT_DATA_TYPE_SIZE;
import static com.exasol.adapter.metadata.DataType.MAX_EXASOL_VARCHAR_SIZE;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.Types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.BaseIdentifierConverter;
import com.exasol.adapter.jdbc.JdbcTypeDescription;
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

    @Test
    void mapText() {
        final JdbcTypeDescription typeDescription = new JdbcTypeDescription(Types.LONGVARCHAR, 0, 16383, 10, "TEXT");
        assertThat(columnMetadataReader.mapJdbcType(typeDescription).getSize(), equalTo(TEXT_DATA_TYPE_SIZE));
    }

    @Test
    void mapMediumText() {
        final JdbcTypeDescription typeDescription = new JdbcTypeDescription(Types.LONGVARCHAR, 0, 4194303, 10,
                "MEDIUMTEXT");
        assertThat(columnMetadataReader.mapJdbcType(typeDescription).getSize(), equalTo(MAX_EXASOL_VARCHAR_SIZE));
    }
}