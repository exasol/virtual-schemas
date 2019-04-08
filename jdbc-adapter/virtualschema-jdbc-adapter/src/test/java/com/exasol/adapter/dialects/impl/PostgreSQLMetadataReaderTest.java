package com.exasol.adapter.dialects.impl;

import static com.exasol.reflect.ReflectionUtils.getMethodReturnViaReflection;
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
    void testTableMetadataReaderClass() {
        assertThat(getMethodReturnViaReflection(this.reader, "createTableMetadataReader"),
                instanceOf(PostgreSQLTableMetadataReader.class));
    }
}