package com.exasol.adapter.jdbc;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.sql.*;
import java.util.List;

import org.json.JSONException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.skyscreamer.jsonassert.JSONAssert;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.BaseIdentifierConverter;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.metadata.DataType.ExaCharset;

class ColumnMetadataReaderTest {
    private static final SQLException FAKE_SQL_EXCEPTION = new SQLException("Fake exception");
    private static final DataType TYPE_MAX_VARCHAR_UTF8 = DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE,
            ExaCharset.UTF8);
    private static final DataType TYPE_MAX_VARCHAR_ASCII = DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE,
            ExaCharset.ASCII);
    private static final String COLUMN_A = "COLUMN_A";
    @Mock
    private Connection connectionMock;
    @Mock
    private DatabaseMetaData remoteMetadataMock;
    @Mock
    private ResultSet columnsMock;

    @BeforeEach
    void beforeEach() throws SQLException {
        MockitoAnnotations.initMocks(this);
        when(this.connectionMock.getMetaData()).thenReturn(this.remoteMetadataMock);
    }

    @Test
    void testMapColumnsSingleColumn() throws SQLException {
        mockDatatype(Types.BOOLEAN);
        final ColumnMetadata column = mapSingleMockedColumn();
        assertAll(() -> assertThat(column.getName(), equalTo(COLUMN_A)),
                () -> assertThat(column.getType(), equalTo(DataType.createBool())),
                () -> assertThat(column.isNullable(), equalTo(true)),
                () -> assertThat(column.isIdentity(), equalTo(false)));
    }

    private void mockDatatype(final int typeId) throws SQLException {
        when(this.columnsMock.getInt(BaseColumnMetadataReader.DATA_TYPE_COLUMN)).thenReturn(typeId);
    }

    private ColumnMetadata mapSingleMockedColumn() throws SQLException {
        when(this.columnsMock.next()).thenReturn(true, false);
        when(this.columnsMock.getString(BaseColumnMetadataReader.NAME_COLUMN)).thenReturn(COLUMN_A);
        when(this.remoteMetadataMock.getColumns(null, null, "THE_TABLE", "%")).thenReturn(this.columnsMock);
        final List<ColumnMetadata> columns = mapMockedColumns(this.columnsMock);
        final ColumnMetadata column = columns.get(0);
        return column;
    }

    private List<ColumnMetadata> mapMockedColumns(final ResultSet columnsMock)
            throws RemoteMetadataReaderException, SQLException {
        when(this.remoteMetadataMock.getColumns(null, null, "THE_TABLE", "%")).thenReturn(columnsMock);
        final List<ColumnMetadata> columns = createDefaultColumnMetadataReader().mapColumns("THE_TABLE");
        return columns;
    }

    protected BaseColumnMetadataReader createDefaultColumnMetadataReader() {
        final BaseColumnMetadataReader reader = new BaseColumnMetadataReader(this.connectionMock,
                AdapterProperties.emptyProperties(), BaseIdentifierConverter.createDefault());
        return reader;
    }

    @Test
    void testParseBoolean() throws SQLException, RemoteMetadataReaderException {
        assertSqlTypeConvertedToExasolType(Types.BOOLEAN, DataType.createBool());
    }

    private void assertSqlTypeConvertedToExasolType(final int typeId, final DataType expectedDataType)
            throws SQLException {
        mockDatatype(typeId);
        assertThat(mapSingleMockedColumn().getType(), equalTo(expectedDataType));
    }

    @Test
    void testParseDate() throws SQLException, RemoteMetadataReaderException {
        assertSqlTypeConvertedToExasolType(Types.DATE, DataType.createDate());
    }

    @ValueSource(ints = { Types.REAL, Types.FLOAT, Types.DOUBLE })
    @ParameterizedTest
    void testParseDouble(final int typeId) throws SQLException, RemoteMetadataReaderException {
        assertSqlTypeConvertedToExasolType(typeId, DataType.createDouble());
    }

    @ValueSource(ints = { Types.CHAR, Types.NCHAR })
    @ParameterizedTest
    void testParseCharWithSize(final int typeId) throws SQLException, RemoteMetadataReaderException {
        final int size = 70;
        assertSqlTypeWithPrecisionConvertedToExasolType(typeId, size, DataType.createChar(size, ExaCharset.UTF8));
    }

    private void assertSqlTypeWithPrecisionConvertedToExasolType(final int typeId, final int expectedPrecision,
            final DataType expectedDataType) throws SQLException {
        mockDatatype(typeId);
        mockSize(expectedPrecision);
        assertThat("Mapping java.sql.Type number " + typeId, mapSingleMockedColumn().getType(),
                equalTo(expectedDataType));
    }

    private void mockSize(final int size) throws SQLException {
        when(this.columnsMock.getInt(BaseColumnMetadataReader.SIZE_COLUMN)).thenReturn(size);
    }

    @ValueSource(ints = { Types.CHAR, Types.NCHAR })
    @ParameterizedTest
    void testParseCharAsciiWithSize(final int typeId) throws SQLException, RemoteMetadataReaderException {
        final int size = 70;
        mockCharOctedLegth(size);
        assertSqlTypeWithPrecisionConvertedToExasolType(typeId, size, DataType.createChar(size, ExaCharset.ASCII));
    }

    @ValueSource(ints = { Types.CHAR, Types.NCHAR })
    @ParameterizedTest
    void testParseCharExceedsMaxCharSize(final int typeId) throws SQLException, RemoteMetadataReaderException {
        final int size = DataType.MAX_EXASOL_CHAR_SIZE + 1;
        assertSqlTypeWithPrecisionConvertedToExasolType(typeId, size, DataType.createVarChar(size, ExaCharset.UTF8));
    }

    @ValueSource(ints = { Types.CHAR, Types.NCHAR })
    @ParameterizedTest
    void testParseCharExceedsMaxVarCharSize(final int typeId) throws SQLException, RemoteMetadataReaderException {
        assertSqlTypeWithPrecisionConvertedToExasolType(typeId, DataType.MAX_EXASOL_VARCHAR_SIZE + 1,
                TYPE_MAX_VARCHAR_UTF8);
    }

    @ValueSource(ints = { Types.VARCHAR, Types.NVARCHAR, Types.LONGVARCHAR, Types.LONGNVARCHAR })
    @ParameterizedTest
    void testParseVarChar(final int typeId) throws SQLException {
        assertSqlTypeConvertedToExasolType(typeId, TYPE_MAX_VARCHAR_ASCII);
    }

    @ValueSource(ints = { Types.VARCHAR, Types.NVARCHAR, Types.LONGVARCHAR, Types.LONGNVARCHAR })
    @ParameterizedTest
    void testParseVarCharWithSize(final int typeId) throws SQLException {
        final int size = 40;
        assertSqlTypeWithPrecisionConvertedToExasolType(typeId, size, DataType.createVarChar(size, ExaCharset.UTF8));
    }

    @ValueSource(ints = { Types.VARCHAR, Types.NVARCHAR, Types.LONGVARCHAR, Types.LONGNVARCHAR })
    @ParameterizedTest
    void testParseVarCharAsciiWithSize(final int typeId) throws SQLException {
        final int size = 80;
        mockCharOctedLegth(80);
        assertSqlTypeWithPrecisionConvertedToExasolType(typeId, size, DataType.createVarChar(size, ExaCharset.ASCII));
    }

    @ValueSource(ints = { Types.VARCHAR, Types.NVARCHAR, Types.LONGVARCHAR, Types.LONGNVARCHAR })
    @ParameterizedTest
    void testParseVarCharExceedsMaxSize(final int typeId) throws SQLException {
        assertSqlTypeWithPrecisionConvertedToExasolType(typeId, DataType.MAX_EXASOL_VARCHAR_SIZE + 1,
                TYPE_MAX_VARCHAR_UTF8);
    }

    private void mockCharOctedLegth(final int length) throws SQLException {
        when(this.columnsMock.getInt(BaseColumnMetadataReader.CHAR_OCTET_LENGTH_COLUMN)).thenReturn(length);
    }

    @Test
    void testParseTimestamp() throws SQLException {
        mockDatatype(Types.TIMESTAMP);
        assertThat(mapSingleMockedColumn().getType().toString(), equalTo("TIMESTAMP"));
    }

    @ValueSource(ints = { Types.TINYINT, Types.SMALLINT })
    @ParameterizedTest
    void testSmallInteger(final int typeId) throws SQLException {
        mockDatatype(typeId);
        final DataType type = mapSingleMockedColumn().getType();
        assertThat(type, equalTo(DataType.createDecimal(9, 0)));
    }

    @ValueSource(ints = { Types.TINYINT, Types.SMALLINT })
    @ParameterizedTest
    void testSmallIntegerWithPrecision(final int typeId) throws SQLException {
        final int precision = 3;
        assertSqlTypeWithPrecisionConvertedToExasolType(typeId, precision, DataType.createDecimal(precision, 0));
    }

    @Test
    void testInteger() throws SQLException {
        assertSqlTypeConvertedToExasolType(Types.INTEGER, DataType.createDecimal(18, 0));
    }

    @Test
    void testIntegerWithPrecision() throws SQLException {
        final int precision = 17;
        assertSqlTypeWithPrecisionConvertedToExasolType(Types.INTEGER, precision, DataType.createDecimal(precision, 0));
    }

    @ValueSource(ints = { Types.INTEGER, Types.BIGINT })
    @ParameterizedTest
    void testIntegerExceedsMaxPrecision(final int typeId) throws SQLException {
        assertSqlTypeWithPrecisionConvertedToExasolType(typeId, DataType.MAX_EXASOL_DECIMAL_PRECISION + 1,
                TYPE_MAX_VARCHAR_UTF8);
    }

    @Test
    void testBigInteger() throws SQLException {
        assertSqlTypeConvertedToExasolType(Types.BIGINT,
                DataType.createDecimal(DataType.MAX_EXASOL_DECIMAL_PRECISION, 0));
    }

    @Test
    void testBigIntegerWithPrecision() throws SQLException {
        final int expectedPrecision = 35;
        assertSqlTypeWithPrecisionConvertedToExasolType(Types.BIGINT, expectedPrecision,
                DataType.createDecimal(expectedPrecision, 0));
    }

    @Test
    void testDecimal() throws SQLException {
        final int precision = 33;
        final int scale = 9;
        mockScale(scale);
        assertSqlTypeWithPrecisionConvertedToExasolType(Types.DECIMAL, precision,
                DataType.createDecimal(precision, scale));
    }

    private void mockScale(final int scale) throws SQLException {
        when(this.columnsMock.getInt(BaseColumnMetadataReader.SCALE_COLUMN)).thenReturn(scale);
    }

    @Test
    void testDecimalExceedsMaxPrecision() throws SQLException {
        final int scale = 17;
        mockScale(scale);
        assertSqlTypeWithPrecisionConvertedToExasolType(Types.DECIMAL, DataType.MAX_EXASOL_DECIMAL_PRECISION + 1,
                TYPE_MAX_VARCHAR_UTF8);
    }

    @Test
    void testNumeric() throws SQLException {
        assertSqlTypeConvertedToExasolType(Types.NUMERIC, TYPE_MAX_VARCHAR_UTF8);
    }

    @Test
    void testTime() throws SQLException {
        assertSqlTypeConvertedToExasolType(Types.TIME, TYPE_MAX_VARCHAR_UTF8);
    }

    @ValueSource(ints = { Types.BINARY, Types.CLOB })
    @ParameterizedTest
    void testBinary(final int typeId) throws SQLException {
        assertSqlTypeConvertedToExasolType(typeId, TYPE_MAX_VARCHAR_UTF8);
    }

    @CsvSource({ RemoteMetadataReaderConstants.JDBC_FALSE + ", false",
            RemoteMetadataReaderConstants.JDBC_TRUE + ", true", "'', true" })
    @ParameterizedTest
    void testMapNotNullableColumn(final String jdbcNullable, final String nullable) throws SQLException {
        mockColumnNotNullable(jdbcNullable);
        mockDatatype(Types.DOUBLE);
        assertThat("JDBC string \"" + jdbcNullable + "\" interpreted as nullable", mapSingleMockedColumn().isNullable(),
                equalTo(Boolean.parseBoolean(nullable)));
    }

    private void mockColumnNotNullable(final String jdbcNullable) throws SQLException {
        when(this.columnsMock.getString(BaseColumnMetadataReader.NULLABLE_COLUMN)).thenReturn(jdbcNullable);
    }

    @Test
    void testMapColumnCountsAsNullableWhenNullabilityCheckThrowsSqlException() throws SQLException {
        mockCheckingNullabilityThrowsSqlException();
        mockDatatype(Types.DOUBLE);
        assertThat(mapSingleMockedColumn().isNullable(), equalTo(true));
    }

    private void mockCheckingNullabilityThrowsSqlException() throws SQLException {
        when(this.columnsMock.getString(BaseColumnMetadataReader.NULLABLE_COLUMN)).thenThrow(FAKE_SQL_EXCEPTION);
    }

    @CsvSource({ RemoteMetadataReaderConstants.JDBC_FALSE + ", false",
            RemoteMetadataReaderConstants.JDBC_TRUE + ", true", "'', false" })
    @ParameterizedTest
    void testMapIdentityColumn(final String jdbcAutoIncrement, final String identity) throws SQLException {
        mockColumnAutoIncrement(jdbcAutoIncrement);
        mockDatatype(Types.DOUBLE);
        assertThat("JDBC string \"" + jdbcAutoIncrement + "\" interpreted as auto-increment on",
                mapSingleMockedColumn().isIdentity(), equalTo(Boolean.parseBoolean(identity)));
    }

    private void mockColumnAutoIncrement(final String jdbcAutoIncrement) throws SQLException {
        when(this.columnsMock.getString(BaseColumnMetadataReader.AUTOINCREMENT_COLUMN)).thenReturn(jdbcAutoIncrement);
    }

    @Test
    void testMapColumnConsideredNotIdentityWhenAutoIncrementCheckThrowsSqlException() throws SQLException {
        mockCheckingAutoIncrementThrowsSqlException();
        mockDatatype(Types.DOUBLE);
        assertThat(mapSingleMockedColumn().isIdentity(), equalTo(false));
    }

    private void mockCheckingAutoIncrementThrowsSqlException() throws SQLException {
        when(this.columnsMock.getString(BaseColumnMetadataReader.AUTOINCREMENT_COLUMN)).thenThrow(FAKE_SQL_EXCEPTION);
    }

    @Test
    void testMapColumnWithDefault() throws SQLException {
        final String defaultValue = "this is a default value";
        mockDefaultValue(defaultValue);
        mockDatatype(Types.VARCHAR);
        assertThat(mapSingleMockedColumn().getDefaultValue(), equalTo(defaultValue));
    }

    private void mockDefaultValue(final String defaultValue) throws SQLException {
        when(this.columnsMock.getString(BaseColumnMetadataReader.DEFAULT_VALUE_COLUMN)).thenReturn(defaultValue);
    }

    @Test
    void testMapColumnWithDefaultNullToEmptyString() throws SQLException {
        mockDefaultValue(null);
        mockDatatype(Types.VARCHAR);
        assertThat(mapSingleMockedColumn().getDefaultValue(), equalTo(""));
    }

    @Test
    void testMapColumnDefaultValueWhenReadingDefaultThrowsSqlException() throws SQLException {
        mockReadingDefaultThrowsSqlException();
        mockDatatype(Types.DOUBLE);
        assertThat(mapSingleMockedColumn().getDefaultValue(), equalTo(""));
    }

    private void mockReadingDefaultThrowsSqlException() throws SQLException {
        when(this.columnsMock.getString(BaseColumnMetadataReader.DEFAULT_VALUE_COLUMN)).thenThrow(FAKE_SQL_EXCEPTION);
    }

    @CsvSource({ "Comment, Comment", "'', ''" })
    @ParameterizedTest
    void testMapColumnWithComment(final String input, final String expected) throws SQLException {
        mockComment(input);
        mockDatatype(Types.VARCHAR);
        assertThat(mapSingleMockedColumn().getComment(), equalTo(expected));
    }

    @Test
    void testMapColumnWithTypeNameNull() throws SQLException {
        mockDatatype(Types.VARCHAR);
        mockTypeName(null);
        assertThat(mapSingleMockedColumn().getOriginalTypeName(), equalTo(""));
    }

    private void mockTypeName(final String typeName) throws SQLException {
        when(this.columnsMock.getString(BaseColumnMetadataReader.TYPE_NAME_COLUMN)).thenReturn(typeName);
    }

    private void mockComment(final String comment) throws SQLException {
        when(this.columnsMock.getString(BaseColumnMetadataReader.REMARKS_COLUMN)).thenReturn(comment);
    }

    @Test
    void testMapColumnWithCommentNullToEmptyString() throws SQLException {
        mockComment(null);
        mockDatatype(Types.VARCHAR);
        assertThat(mapSingleMockedColumn().getComment(), equalTo(""));
    }

    @Test
    void testMapColumnCommentWhenReadingDefaultThrowsSqlException() throws SQLException {
        mockReadingCommentThrowsSqlException();
        mockDatatype(Types.DOUBLE);
        assertThat(mapSingleMockedColumn().getComment(), equalTo(""));
    }

    private void mockReadingCommentThrowsSqlException() throws SQLException {
        when(this.columnsMock.getString(BaseColumnMetadataReader.REMARKS_COLUMN)).thenThrow(FAKE_SQL_EXCEPTION);
    }

    @Test
    void testMapColumnsWrapsSqlException() throws SQLException {
        when(this.connectionMock.getMetaData()).thenThrow(FAKE_SQL_EXCEPTION);
        assertThrows(RemoteMetadataReaderException.class, () -> createDefaultColumnMetadataReader().mapColumns(""));
    }

    @Test
    void testMapColumnAdapterNotes() throws SQLException, JSONException {
        mockDatatype(Types.DOUBLE);
        mockTypeName("DOUBLE");
        JSONAssert.assertEquals(mapSingleMockedColumn().getAdapterNotes(),
                "{\"jdbcDataType\":8, \"typeName\":\"DOUBLE\"}", true);
    }
}