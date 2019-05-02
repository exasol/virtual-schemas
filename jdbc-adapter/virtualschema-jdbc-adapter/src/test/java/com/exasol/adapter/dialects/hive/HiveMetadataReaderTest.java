package com.exasol.adapter.dialects.hive;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.jdbc.IdentifierCaseHandling;

public class HiveMetadataReaderTest {
    private HiveMetadataReader hiveMetadataReader;

    @BeforeEach
    void beforeEach() {
        this.hiveMetadataReader = new HiveMetadataReader(null, AdapterProperties.emptyProperties());
    }

    @Test
    void testCreateIdentifierConverter() {
        final IdentifierConverter converter = this.hiveMetadataReader.createIdentifierConverter();
        assertAll(
                () -> assertThat(converter.getQuotedIdentifierHandling(),
                        equalTo(IdentifierCaseHandling.INTERPRET_AS_LOWER)),
                () -> assertThat(converter.getUnquotedIdentifierHandling(),
                        equalTo(IdentifierCaseHandling.INTERPRET_AS_LOWER)));

    }
}