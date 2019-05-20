package com.exasol.adapter.dialects.bigquery;

import com.exasol.adapter.*;
import com.exasol.adapter.jdbc.*;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.sql.*;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class BigQueryMetadataReaderTest {
    private BigQueryMetadataReader reader;
    @Mock
    private Connection connectionMock;

    @BeforeEach
    void beforeEach() {
        this.reader = new BigQueryMetadataReader(this.connectionMock,
                AdapterProperties.emptyProperties());
    }

    @Test
    void testGetTableMetadataReader() {
        assertThat(this.reader.getTableMetadataReader(), instanceOf(BaseTableMetadataReader.class));
    }

    @Test
    void testGetColumnMetadataReader() {
        assertThat(this.reader.getColumnMetadataReader(),
                instanceOf(BaseColumnMetadataReader.class));
    }
}