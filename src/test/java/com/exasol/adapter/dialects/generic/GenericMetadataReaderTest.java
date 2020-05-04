package com.exasol.adapter.dialects.generic;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import java.sql.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.exasol.adapter.AdapterProperties;

@ExtendWith(MockitoExtension.class)
class GenericMetadataReaderTest {
    private GenericMetadataReader reader;
    @Mock
    private Connection connectionMock;
    @Mock
    private DatabaseMetaData metadataMock;

    @BeforeEach
    void beforeEach() throws SQLException {
        when(this.connectionMock.getMetaData()).thenReturn(this.metadataMock);
        when(this.metadataMock.supportsMixedCaseIdentifiers()).thenReturn(true);
        when(this.metadataMock.supportsMixedCaseQuotedIdentifiers()).thenReturn(true);
        this.reader = new GenericMetadataReader(this.connectionMock, AdapterProperties.emptyProperties());
    }

    @Test
    void testGetIdentifierConverter() {
        assertThat(this.reader.getIdentifierConverter(), instanceOf(GenericIdentifierConverter.class));
    }
}