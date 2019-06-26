package com.exasol.adapter.dialects.saphana;

import com.exasol.adapter.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.dialects.db2.*;
import com.exasol.adapter.metadata.*;
import org.hamcrest.*;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class SapHanaColumnMetadataReaderTest {
    private SapHanaColumnMetadataReader sapHanaMetadataReader;

    @BeforeEach
    void beforeEach() {
        this.sapHanaMetadataReader = new SapHanaColumnMetadataReader(null, AdapterProperties.emptyProperties(),
                BaseIdentifierConverter.createDefault());
    }

    @Test
    void mapAsDecimal() {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(Types.DECIMAL, 5, 18, 0, "");
        assertThat(this.sapHanaMetadataReader.mapJdbcType(jdbcTypeDescription), equalTo(DataType.createDecimal(18, 5)));
    }

    @Test
    void mapAsDouble() {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(Types.DECIMAL, 0, 18, 0, "");
        assertThat(this.sapHanaMetadataReader.mapJdbcType(jdbcTypeDescription), equalTo(DataType.createDouble()));
    }


    @Test
    void testMapJdbcTypeDefault() {
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(Types.BOOLEAN, 0, 0, 0, "BOOLEAN");
        assertThat(this.sapHanaMetadataReader.mapJdbcType(jdbcTypeDescription),
              CoreMatchers.equalTo(DataType.createBool()));
    }
}