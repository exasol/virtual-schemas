package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.BaseTableMetadataReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

class DB2MetadataReaderTest {
    private DB2MetadataReader db2MetadataReader;

    @BeforeEach
    void beforeEach() {
        this.db2MetadataReader = new DB2MetadataReader(null, AdapterProperties.emptyProperties());
    }

    @Test
    void testGetTableMetadataReader() {
        assertThat(this.db2MetadataReader.getTableMetadataReader(), instanceOf(BaseTableMetadataReader.class));
    }

    @Test
    void testGetColumnMetadataReader() {
        assertThat(this.db2MetadataReader.getColumnMetadataReader(), instanceOf(DB2ColumnMetadataReader.class));
    }
}
