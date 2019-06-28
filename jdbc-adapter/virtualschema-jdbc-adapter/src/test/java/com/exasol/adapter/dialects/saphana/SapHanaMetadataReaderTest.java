package com.exasol.adapter.dialects.saphana;

import com.exasol.adapter.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.dialects.bigquery.*;
import com.exasol.adapter.jdbc.*;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.sql.*;

import static com.exasol.adapter.jdbc.RemoteMetadataReaderConstants.ANY_TABLE_TYPE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class SapHanaMetadataReaderTest {
    private SapHanaMetadataReader reader;
    @Mock
    private Connection connectionMock;

    @BeforeEach
    void beforeEach() {
        this.reader = new SapHanaMetadataReader(this.connectionMock, AdapterProperties.emptyProperties());
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
        assertThat(this.reader.getSupportedTableTypes(), equalTo(ANY_TABLE_TYPE));
    }

    @Test
    void testCreateIdentifierConverter() {
        final IdentifierConverter converter = this.reader.getIdentifierConverter();
        assertAll(() -> assertThat(converter, instanceOf(BaseIdentifierConverter.class)),
              () -> assertThat(converter.getQuotedIdentifierHandling(),
                    equalTo(IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE)),
              () -> assertThat(converter.getUnquotedIdentifierHandling(),
                    equalTo(IdentifierCaseHandling.INTERPRET_AS_UPPER)));
    }
}