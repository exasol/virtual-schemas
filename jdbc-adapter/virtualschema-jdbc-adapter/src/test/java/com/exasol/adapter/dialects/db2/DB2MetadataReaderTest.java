package com.exasol.adapter.dialects.db2;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.BaseTableMetadataReader;

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
