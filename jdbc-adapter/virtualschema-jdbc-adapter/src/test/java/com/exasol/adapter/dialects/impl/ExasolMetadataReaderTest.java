package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.BaseTableMetadataReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

class ExasolMetadataReaderTest {
    private ExasolMetadataReader exasolMetadataReader;

    @BeforeEach
    void beforeEach() {
        this.exasolMetadataReader = new ExasolMetadataReader(null, AdapterProperties.emptyProperties());
    }

    @Test
    void testGetTableMetadataReader() {
        assertThat(this.exasolMetadataReader.getTableMetadataReader(), instanceOf(BaseTableMetadataReader.class));
    }

    @Test
    void testGetColumnMetadataReader() {
        assertThat(this.exasolMetadataReader.getColumnMetadataReader(), instanceOf(ExasolColumnMetadataReader.class));
    }
}
