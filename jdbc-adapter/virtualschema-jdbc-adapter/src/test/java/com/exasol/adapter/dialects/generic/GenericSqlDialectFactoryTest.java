package com.exasol.adapter.dialects.generic;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;

public class GenericSqlDialectFactoryTest {
    private GenericSqlDialectFactory factory;

    @BeforeEach
    void beforeEach() {
        this.factory = new GenericSqlDialectFactory();
    }

    @Test
    void testGetName() {
        assertThat(this.factory.getSqlDialectName(), equalTo("GENERIC"));
    }

    @Test
    void testCreateDialect() throws SQLException {
        final Connection connectionMock = mock(Connection.class);
        final DatabaseMetaData metadataMock = mock(DatabaseMetaData.class);
        when(connectionMock.getMetaData()).thenReturn(metadataMock);
        when(metadataMock.supportsMixedCaseIdentifiers()).thenReturn(true);
        when(metadataMock.supportsMixedCaseQuotedIdentifiers()).thenReturn(true);
        assertThat(this.factory.createSqlDialect(connectionMock, AdapterProperties.emptyProperties()),
                instanceOf(GenericSqlDialect.class));
    }
}