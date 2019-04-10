package com.exasol.adapter.dialects.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.sql.Types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.JdbcTypeDescription;
import com.exasol.adapter.metadata.DataType;

class DB2ColumnMetadataReaderTest {
    private DB2ColumnMetadataReader db2ColumnMetadataReader;

    @BeforeEach
    void beforeEach() {
        this.db2ColumnMetadataReader = new DB2ColumnMetadataReader(null, AdapterProperties.emptyProperties());
    }

    @Test
    void testMapJdbcTypeClob() {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(Types.CLOB, 0, 0, 0, "");
        assertThat(this.db2ColumnMetadataReader.mapJdbcType(jdbcTypeDescription),
                equalTo(DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, DataType.ExaCharset.UTF8)));
    }

    @Test
    void testMapJdbcTypeIntervalVarcharWithMaxSize() {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(
                DB2ColumnMetadataReader.DB2SQL_VARCHAR_WITH_EXASOL_MAX_SIZE, 0, 0, 0, "");
        assertThat(this.db2ColumnMetadataReader.mapJdbcType(jdbcTypeDescription),
                equalTo(DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, DataType.ExaCharset.UTF8)));
    }

    @Test
    void testMapJdbcTypetimestamp() {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(Types.TIMESTAMP, 0, 0, 0, "");
        assertThat(this.db2ColumnMetadataReader.mapJdbcType(jdbcTypeDescription),
                equalTo(DataType.createVarChar(32, DataType.ExaCharset.UTF8)));
    }

    @ValueSource(ints = { Types.VARCHAR, Types.NVARCHAR, Types.LONGNVARCHAR, Types.CHAR, Types.NCHAR,
            Types.LONGNVARCHAR })
    @ParameterizedTest
    void testMapJdbcVarcharSizeLesserThanExasolMaxVarcharSize(final int type) {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(type, 0, 10, 0, "");
        assertThat(this.db2ColumnMetadataReader.mapJdbcType(jdbcTypeDescription),
                equalTo(DataType.createVarChar(10, DataType.ExaCharset.UTF8)));
    }

    @ValueSource(ints = { Types.VARCHAR, Types.NVARCHAR, Types.LONGNVARCHAR, Types.CHAR, Types.NCHAR,
            Types.LONGNVARCHAR })
    @ParameterizedTest
    void testMapJdbcVarcharSizeGreaterThanExasolMaxVarcharSize(final int type) {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(type, 0,
                DataType.MAX_EXASOL_VARCHAR_SIZE + 1, 0, "");
        assertThat(this.db2ColumnMetadataReader.mapJdbcType(jdbcTypeDescription),
                equalTo(DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, DataType.ExaCharset.UTF8)));
    }

    @Test
    void testMapJdbcTypeDB2SqlChar() {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(DB2ColumnMetadataReader.DB2SQL_CHAR,
                0, 0, 0, "");
        assertThat(this.db2ColumnMetadataReader.mapJdbcType(jdbcTypeDescription),
                equalTo(DataType.createChar(jdbcTypeDescription.getPrecisionOrSize() * 2, DataType.ExaCharset.ASCII)));
    }

    @Test
    void testMapJdbcTypeDB2SqlVarchar() {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(DB2ColumnMetadataReader.DB2SQL_VARCHAR,
              0, 0, 0, "");
        assertThat(this.db2ColumnMetadataReader.mapJdbcType(jdbcTypeDescription),
              equalTo(DataType.createVarChar(jdbcTypeDescription.getPrecisionOrSize() * 2, DataType.ExaCharset.ASCII)));
    }
}