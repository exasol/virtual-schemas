package com.exasol.adapter.dialects.impala;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.dialects.hive.HiveMetadataReader;
import com.exasol.adapter.jdbc.IdentifierCaseHandling;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class ImpalaMetadataReaderTest {
    private ImpalaMetadataReader impalaMetadataReader;

    @BeforeEach
    void beforeEach() {
        this.impalaMetadataReader = new ImpalaMetadataReader(null, AdapterProperties.emptyProperties());
    }

    @Test
    void testCreateIdentifierConverter() {
        final IdentifierConverter converter = this.impalaMetadataReader.createIdentifierConverter();
        assertAll(
                () -> assertThat(converter.getQuotedIdentifierHandling(),
                        equalTo(IdentifierCaseHandling.INTERPRET_AS_LOWER)),
                () -> assertThat(converter.getUnquotedIdentifierHandling(),
                        equalTo(IdentifierCaseHandling.INTERPRET_AS_LOWER)));

    }
}