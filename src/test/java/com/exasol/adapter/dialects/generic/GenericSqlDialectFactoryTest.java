package com.exasol.adapter.dialects.generic;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.when;

import java.sql.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.ConnectionFactory;

@ExtendWith(MockitoExtension.class)
class GenericSqlDialectFactoryTest {
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
    void testCreateDialect(@Mock final DatabaseMetaData metadataMock, @Mock final Connection connectionMock,
            @Mock final ConnectionFactory connectionFactoryMock) throws SQLException {
        when(metadataMock.supportsMixedCaseIdentifiers()).thenReturn(true);
        when(metadataMock.supportsMixedCaseQuotedIdentifiers()).thenReturn(true);
        when(connectionMock.getMetaData()).thenReturn(metadataMock);
        when(connectionFactoryMock.getConnection()).thenReturn(connectionMock);
        assertThat(this.factory.createSqlDialect(connectionFactoryMock, AdapterProperties.emptyProperties()),
                instanceOf(GenericSqlDialect.class));
    }
}