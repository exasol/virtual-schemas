package com.exasol.adapter.dialects.sqlserver;

import static com.exasol.adapter.dialects.sqlserver.SqlServerColumnMetadataReader.*;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.sql.Types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.metadata.DataType.ExaCharset;

class SqlServerColumnMetadataReaderTest extends AbstractColumnMetadataReaderTest {
    @BeforeEach
    void beforeEach() {
        this.columnMetadataReader = new SqlServerColumnMetadataReader(null, AdapterProperties.emptyProperties(),
                BaseIdentifierConverter.createDefault());
    }

    @Test
    void testMapJdbcTypeVarChar() {
        final int expectedSize = 42;
        final DataType type = this.columnMetadataReader
                .mapJdbcType(new JdbcTypeDescription(Types.VARCHAR, 0, expectedSize, expectedSize, ""));
        assertThat(type, equalTo(DataType.createVarChar(expectedSize, ExaCharset.ASCII)));
    }

    @Test
    void testMapJdbcTypeVarCharRepresentingDate() {
        final DataType type = mapJdbcTypeWithName(Types.VARCHAR, SQLSERVER_DATE_TYPE_NAME);
        assertThat(type, equalTo(DataType.createDate()));
    }

    @Test
    void testMapJdbcTypeVarCharRepresentingDatetime2() {
        final DataType type = mapJdbcTypeWithName(Types.VARCHAR, SQLSERVER_DATETIME2_TYPE_NAME);
        assertThat(type, equalTo(DataType.createTimestamp(false)));
    }

    @Test
    void testMapJdbcTypeTime() {
        final DataType jdbcType = mapJdbcType(Types.TIME);
        assertThat(jdbcType, equalTo(DataType.createVarChar(SQLSERVER_TIMESTAMP_TEXT_SIZE, DataType.ExaCharset.UTF8)));
    }

    @Test
    void testMapJdbcTypeTimeWithTimezone() {
        final DataType jdbcType = mapJdbcType(Types.TIME_WITH_TIMEZONE);
        assertThat(jdbcType, equalTo(DataType.createVarChar(SQLSERVER_TIMESTAMP_TEXT_SIZE, DataType.ExaCharset.UTF8)));
    }

    @Test
    void testMapJdbcTypeNumeric() {
        assertNumericMappedToDecimalWithPrecisionAndScale(DataType.MAX_EXASOL_DECIMAL_PRECISION, 2);
    }

    @Test
    void testMapJdbcTypeNumericExceedingExsolMaxPrecisionToDouble() {
        assertNumericMappedToDoubleWithPrecsionAndScale(DataType.MAX_EXASOL_DECIMAL_PRECISION + 1, 2);
    }

    @Test
    void testMapJdbcTypeOther() {
        final DataType jdbcType = mapJdbcType(Types.OTHER);
        assertThat(jdbcType, equalTo(DataType.createVarChar(SQLSERVER_MAX_VARCHAR_SIZE, DataType.ExaCharset.UTF8)));
    }

    @Test
    void testMapJdbcTypeSqlXml() {
        final DataType jdbcType = mapJdbcType(Types.SQLXML);
        assertThat(jdbcType, equalTo(DataType.createVarChar(SQLSERVER_MAX_VARCHAR_SIZE, DataType.ExaCharset.UTF8)));
    }

    @Test
    void testMapJdbcTypeClob() {
        final DataType jdbcType = mapJdbcType(Types.CLOB);
        assertThat(jdbcType, equalTo(DataType.createVarChar(SQLSERVER_MAX_CLOB_SIZE, DataType.ExaCharset.UTF8)));
    }

    @ValueSource(ints = { Types.BLOB, Types.VARBINARY, Types.BINARY, Types.DISTINCT })
    @ParameterizedTest
    void testMapJdbcTypeBlob(final int type) {
        final DataType jdbcType = mapJdbcType(type);
        assertThat(jdbcType, equalTo(DataType.createVarChar(SQLSERVER_BLOB_SIZE, DataType.ExaCharset.UTF8)));
    }

    @Test
    void testMapJdbcTypeHierarchyId() {
        final DataType jdbcType = mapJdbcTypeWithName(Types.BLOB, SQLSERVER_HIERARCHYID_TYPE_NAME);
        assertThat(jdbcType, equalTo(DataType.createVarChar(SQLSERVER_HIERARCHYID_SIZE, DataType.ExaCharset.UTF8)));
    }

    @Test
    void testMapJdbcTypeGeometry() {
        final DataType jdbcType = mapJdbcTypeWithName(Types.BLOB, SQLSERVER_GEOMETRY_TYPE_NAME);
        assertThat(jdbcType, equalTo(DataType.createVarChar(SQLSERVER_MAX_VARCHAR_SIZE, DataType.ExaCharset.UTF8)));
    }

    @Test
    void testMapJdbdTypeFallbackToParent() {
        final DataType jdbcType = mapJdbcType(Types.BOOLEAN);
        assertThat(jdbcType, equalTo(DataType.createBool()));
    }
}
