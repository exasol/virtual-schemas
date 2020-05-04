package com.exasol.adapter.dialects.impala;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierCaseHandling;
import com.exasol.adapter.dialects.IdentifierConverter;

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