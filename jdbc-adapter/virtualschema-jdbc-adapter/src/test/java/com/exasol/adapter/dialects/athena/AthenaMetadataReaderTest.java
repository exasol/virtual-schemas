package com.exasol.adapter.dialects.athena;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.sql.Connection;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.BaseIdentifierConverter;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.jdbc.*;

@ExtendWith(MockitoExtension.class)
public class AthenaMetadataReaderTest {
    private AthenaMetadataReader reader;
    @Mock
    private Connection connectionMock;

    @BeforeEach
    void beforeEach() {
        this.reader = new AthenaMetadataReader(this.connectionMock, AdapterProperties.emptyProperties());
    }

    @Test
    void testGetTableMetadataReader() {
        assertThat(this.reader.getTableMetadataReader(), instanceOf(BaseTableMetadataReader.class));
    }

    @Test
    void testGetColumnMetadataReader() {
        assertThat(this.reader.getColumnMetadataReader(), instanceOf(BaseColumnMetadataReader.class));
    }

    @Test
    void testGetSupportedTableTypes() {
        assertThat(this.reader.getSupportedTableTypes(), emptyIterableOf(String.class));
    }

    @Test
    void testCreateIdentifierConverter() {
        final IdentifierConverter converter = this.reader.getIdentifierConverter();
        assertAll(() -> assertThat(converter, instanceOf(BaseIdentifierConverter.class)),
                () -> assertThat(converter.getQuotedIdentifierHandling(),
                        equalTo(IdentifierCaseHandling.INTERPRET_AS_UPPER)),
                () -> assertThat(converter.getUnquotedIdentifierHandling(),
                        equalTo(IdentifierCaseHandling.INTERPRET_AS_UPPER)));
    }
}