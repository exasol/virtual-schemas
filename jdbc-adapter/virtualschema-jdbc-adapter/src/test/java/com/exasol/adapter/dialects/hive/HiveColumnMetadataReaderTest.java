package com.exasol.adapter.dialects.hive;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.*;

import java.sql.*;

import org.junit.jupiter.api.*;

import com.exasol.adapter.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.metadata.*;

class HiveColumnMetadataReaderTest {
    private HiveColumnMetadataReader columnMetadataReader;

    @BeforeEach
    void beforeEach() {
        this.columnMetadataReader = new HiveColumnMetadataReader(null, AdapterProperties.emptyProperties(),
                BaseIdentifierConverter.createDefault());
    }

    @Test
    void mapDecimalReturnDecimal() {
        final JdbcTypeDescription typeDescription = new JdbcTypeDescription(Types.DECIMAL, 0,
                DataType.MAX_EXASOL_DECIMAL_PRECISION, 10, "DECIMAL");
        assertThat(columnMetadataReader.mapJdbcType(typeDescription), equalTo(DataType.createDecimal(36, 0)));
    }

    @Test
    void mapDecimalReturnVarchar() {
        final JdbcTypeDescription typeDescription = new JdbcTypeDescription(Types.DECIMAL, 0,
                DataType.MAX_EXASOL_DECIMAL_PRECISION + 1, 10, "DECIMAL");
        assertThat(columnMetadataReader.mapJdbcType(typeDescription),
                equalTo(DataType.createMaximumSizeVarChar(DataType.ExaCharset.UTF8)));
    }
}