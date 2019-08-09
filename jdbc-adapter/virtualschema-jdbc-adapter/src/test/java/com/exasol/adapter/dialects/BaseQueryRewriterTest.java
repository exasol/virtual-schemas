package com.exasol.adapter.dialects;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.*;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.dummy.DummySqlDialect;
import com.exasol.adapter.jdbc.BaseRemoteMetadataReader;
import com.exasol.adapter.sql.TestSqlStatementFactory;

class BaseQueryRewriterTest extends AbstractQueryRewriterTest {
    @BeforeEach
    void beforeEach() {
        this.exaConnectionInformation = mock(ExaConnectionInformation.class);
        this.exaMetadata = mock(ExaMetadata.class);
        this.rawProperties = new HashMap<>();
        this.statement = TestSqlStatementFactory.createSelectOneFromDual();
    }

    @Test
    void testRewriteWithJdbcConnection() throws AdapterException, SQLException, ExaConnectionAccessException {
        mockExasolNamedConnection();
        final Connection connectionMock = mockConnection();
        setConnectionNameProperty();
        final AdapterProperties properties = new AdapterProperties(this.rawProperties);
        final SqlDialect dialect = new DummySqlDialect(connectionMock, properties);
        final BaseRemoteMetadataReader metadataReader = new BaseRemoteMetadataReader(connectionMock, properties);
        final QueryRewriter queryRewriter = new BaseQueryRewriter(dialect, metadataReader, connectionMock);
        assertThat(queryRewriter.rewrite(this.statement, this.exaMetadata, properties),
                equalTo("IMPORT INTO (c1 DECIMAL(18, 0)) FROM JDBC AT " + CONNECTION_NAME
                        + " STATEMENT 'SELECT 1 FROM \"DUAL\"'"));
    }
}