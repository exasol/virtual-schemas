package com.exasol.adapter.dialects.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.BaseTableMetadataReader;

class TeradataMetadataReaderTest {
    private TeradataMetadataReader teradataMetadataReader;

    @BeforeEach
    void beforeEach() {
        this.teradataMetadataReader = new TeradataMetadataReader(null, AdapterProperties.emptyProperties());
    }

    @Test
    void testGetTableMetadataReader() {
        assertThat(this.teradataMetadataReader.getTableMetadataReader(), instanceOf(BaseTableMetadataReader.class));
    }

    @Test
    void testGetColumnMetadataReader() {
        assertThat(this.teradataMetadataReader.getColumnMetadataReader(),
                instanceOf(TeradataColumnMetadataReader.class));
    }
}