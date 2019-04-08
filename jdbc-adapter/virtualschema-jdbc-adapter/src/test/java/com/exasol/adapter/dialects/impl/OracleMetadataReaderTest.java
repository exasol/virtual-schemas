package com.exasol.adapter.dialects.impl;

import static com.exasol.reflect.ReflectionUtils.getMethodReturnViaReflection;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import java.lang.reflect.InvocationTargetException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;

class OracleMetadataReaderTest {
    private OracleMetadataReader reader;

    @BeforeEach
    void beforeEach() {
        this.reader = new OracleMetadataReader(null, AdapterProperties.emptyProperties());
    }

    @Test
    void testTableMetadataReaderClass() throws NoSuchFieldException, IllegalAccessException, NoSuchMethodException,
            SecurityException, IllegalArgumentException, InvocationTargetException {
        assertThat(getMethodReturnViaReflection(this.reader, "createTableMetadataReader"),
                instanceOf(OracleTableMetadataReader.class));
    }
}
