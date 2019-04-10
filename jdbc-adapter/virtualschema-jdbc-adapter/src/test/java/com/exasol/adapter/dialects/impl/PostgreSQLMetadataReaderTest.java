package com.exasol.adapter.dialects.impl;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;

class PostgreSQLMetadataReaderTest {
    private PostgreSQLMetadataReader reader;

    @BeforeEach
    void beforeEach() {
        this.reader = new PostgreSQLMetadataReader(null, AdapterProperties.emptyProperties());
    }

    @Test
    void testGetTableMetadataReader() {
        assertThat(this.reader.getTableMetadataReader(), instanceOf(PostgreSQLTableMetadataReader.class));
    }

    @Test
    void testGetColumnMetadataReader() {
        assertThat(this.reader.getColumnMetadataReader(), instanceOf(PostgreSQLColumnMetadataReader.class));
    }
}