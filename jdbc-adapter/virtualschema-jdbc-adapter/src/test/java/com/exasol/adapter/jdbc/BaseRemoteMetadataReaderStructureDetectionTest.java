package com.exasol.adapter.jdbc;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import com.exasol.adapter.AdapterProperties;

@ExtendWith(MockitoExtension.class)
class BaseRemoteMetadataReaderStructureDetectionTest {
    @Test
    void testGetCatalogName() {
        final Map<String, String> rawProperties = new HashMap<>();
        final String expectedCatalog = "CATALOG_a";
        rawProperties.put(AdapterProperties.CATALOG_NAME_PROPERTY, expectedCatalog);
        final ColumnMetadataReader reader = new BaseColumnMetadataReader(null, new AdapterProperties(rawProperties));
        assertThat(reader.getCatalogName(), equalTo(expectedCatalog));
    }

    @Test
    void testGetSchemaName() {
        final Map<String, String> rawProperties = new HashMap<>();
        final String expectedSchema = "SCHEMA_X";
        rawProperties.put(AdapterProperties.CATALOG_NAME_PROPERTY, expectedSchema);
        final ColumnMetadataReader reader = new BaseColumnMetadataReader(null, new AdapterProperties(rawProperties));
        assertThat(reader.getSchemaName(), equalTo(expectedSchema));
    }
}