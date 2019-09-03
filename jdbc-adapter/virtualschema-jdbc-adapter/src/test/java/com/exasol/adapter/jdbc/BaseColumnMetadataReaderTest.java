package com.exasol.adapter.jdbc;

import static com.exasol.adapter.dialects.hive.HiveProperties.HIVE_CAST_NUMBER_TO_DECIMAL_PROPERTY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import com.exasol.adapter.dialects.hive.*;
import com.exasol.adapter.metadata.*;
import org.junit.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.BaseIdentifierConverter;
import com.exasol.adapter.dialects.JdbcTypeDescription;
import com.exasol.adapter.metadata.DataType.ExaDataType;

public class BaseColumnMetadataReaderTest {
    private BaseColumnMetadataReader reader;

    @BeforeEach
    void beforeEach() {
        this.reader = new BaseColumnMetadataReader(null, AdapterProperties.emptyProperties(),
                new BaseIdentifierConverter(IdentifierCaseHandling.INTERPRET_AS_UPPER,
                        IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE));
    }

    @ValueSource(ints = { Types.OTHER, Types.BLOB, Types.NCLOB, Types.LONGVARBINARY, Types.VARBINARY, Types.JAVA_OBJECT,
            Types.DISTINCT, Types.STRUCT, Types.ARRAY, Types.REF, Types.DATALINK, Types.SQLXML, Types.NULL,
            Types.REF_CURSOR })
    @ParameterizedTest
    void testMappingUnsupportedTypesReturnsUnsupportedType(final int jdbcType) {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(jdbcType, 0, 0, 0, null);
        assertThat(this.reader.mapJdbcType(jdbcTypeDescription).getExaDataType(), equalTo(ExaDataType.UNSUPPORTED));
    }

    @Test
    void testGetColumnsFromResultSetSkipsUnsupportedColumns() throws SQLException {
        final ResultSet remoteColumnsMock = mock(ResultSet.class);
        when(remoteColumnsMock.next()).thenReturn(true, true, true, false);
        when(remoteColumnsMock.getString(BaseColumnMetadataReader.NAME_COLUMN)).thenReturn("DATE_COL", "BLOB_COL",
                "DOUBLE_COL");
        when(remoteColumnsMock.getInt(BaseColumnMetadataReader.DATA_TYPE_COLUMN)).thenReturn(Types.DATE, Types.BLOB,
                Types.DOUBLE);
        final List<ColumnMetadata> columns = this.reader.getColumnsFromResultSet(remoteColumnsMock);
        final List<ExaDataType> columnTypes = columns //
                .stream() //
                .map(column -> column.getType().getExaDataType()) //
                .collect(Collectors.toList());
        assertThat(columnTypes, containsInAnyOrder(ExaDataType.DATE, ExaDataType.DOUBLE));
    }

    @Test
    void testMapColumnTypeWithMaxExasolDecimalPrecision() {
        final int precision = DataType.MAX_EXASOL_DECIMAL_PRECISION;
        final int scale = 0;
        final JdbcTypeDescription typeDescription = new JdbcTypeDescription(Types.DECIMAL, scale, precision, 10,
                "DECIMAL");
        Assert.assertThat(this.reader.mapJdbcType(typeDescription), equalTo(DataType.createDecimal(precision, scale)));
    }

    @Test
    void testMapColumnTypeBeyondMaxExasolDecimalPrecision() {
        final JdbcTypeDescription typeDescription = new JdbcTypeDescription(Types.DECIMAL, 0,
                DataType.MAX_EXASOL_DECIMAL_PRECISION + 1, 10, "DECIMAL");
        Assert.assertThat(this.reader.mapJdbcType(typeDescription),
                equalTo(DataType.createMaximumSizeVarChar(DataType.ExaCharset.UTF8)));
    }

    @Test
    void testMapColumnTypeBeyondMaxExasolDecimalPrecisionWithCastProperty() {
        final Map<String, String> rawProperties = new HashMap<>();
        rawProperties.put(HIVE_CAST_NUMBER_TO_DECIMAL_PROPERTY, "10,2");
        final AdapterProperties properties = new AdapterProperties(rawProperties);
        final ColumnMetadataReader columnMetadataReader = new HiveColumnMetadataReader(null, properties,
                BaseIdentifierConverter.createDefault());
        final JdbcTypeDescription typeDescription = new JdbcTypeDescription(Types.DECIMAL, 0,
                DataType.MAX_EXASOL_DECIMAL_PRECISION + 1, 10, "DECIMAL");
        Assert.assertThat(columnMetadataReader.mapJdbcType(typeDescription), equalTo(DataType.createDecimal(10, 2)));
    }
}