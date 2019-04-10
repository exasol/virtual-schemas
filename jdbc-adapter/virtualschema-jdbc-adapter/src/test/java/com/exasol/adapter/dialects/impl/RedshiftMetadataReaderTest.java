package com.exasol.adapter.dialects.impl;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.BaseTableMetadataReader;

class RedshiftMetadataReaderTest {
    private RedshiftMetadataReader reader;

    @BeforeEach
    void beforeEach() {
        this.reader = new RedshiftMetadataReader(null, AdapterProperties.emptyProperties());
    }

    @Test
    void testGetTableMetadataReader() {
        assertThat(this.reader.getTableMetadataReader(), instanceOf(BaseTableMetadataReader.class));
    }

    @Test
    void testGetColumnMetadataReader() {
        assertThat(this.reader.getColumnMetadataReader(), instanceOf(RedshiftColumnMetadataReader.class));
    }
}