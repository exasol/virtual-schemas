package com.exasol.adapter.dialects.bigquery;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.Types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.BaseIdentifierConverter;
import com.exasol.adapter.jdbc.JdbcTypeDescription;
import com.exasol.adapter.metadata.DataType;

class BigQueryColumnMetadataReaderTest {
    private BigQueryColumnMetadataReader columnMetadataReader;

    @BeforeEach
    void beforeEach() {
        this.columnMetadataReader = new BigQueryColumnMetadataReader(null, AdapterProperties.emptyProperties(),
                BaseIdentifierConverter.createDefault());
    }

    @Test
    void mapDecimalReturnDecimal() {
        final JdbcTypeDescription typeDescription = new JdbcTypeDescription(Types.TIME, 0, 0, 10, "TIME");
        assertThat(this.columnMetadataReader.mapJdbcType(typeDescription),
                equalTo(DataType.createVarChar(30, DataType.ExaCharset.UTF8)));
    }
}