package com.exasol.adapter.dialects.sybase;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.sqlserver.SqlServerColumnMetadataReader;
import com.exasol.adapter.jdbc.BaseTableMetadataReader;
import com.exasol.adapter.jdbc.RemoteMetadataReader;

class SybaseMetadataReaderTest {
    private RemoteMetadataReader reader;

    @BeforeEach
    void beforeEach() {
        this.reader = new SybaseMetadataReader(null, AdapterProperties.emptyProperties());
    }

    @Test
    void testGetTableMetadataReader() {
        assertThat(this.reader.getTableMetadataReader(), instanceOf(BaseTableMetadataReader.class));
    }

    @Test
    void testGetColumnMetadataReaderReusesSqlServerColumnMetadataReader() {
        assertThat(this.reader.getColumnMetadataReader(), instanceOf(SqlServerColumnMetadataReader.class));
    }
}