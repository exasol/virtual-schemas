package com.exasol.adapter.dialects.oracle;

import static com.exasol.adapter.AdapterProperties.*;
import static com.exasol.adapter.dialects.oracle.OracleProperties.ORACLE_CONNECTION_NAME_PROPERTY;
import static com.exasol.adapter.dialects.oracle.OracleProperties.ORACLE_IMPORT_PROPERTY;
import static com.exasol.reflect.ReflectionUtils.getMethodReturnViaReflection;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
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
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.dialects.exasol.ExasolSqlDialect;
import com.exasol.adapter.jdbc.BaseRemoteMetadataReader;
import com.exasol.adapter.sql.TestSqlStatementFactory;

public class OracleQueryRewriterTest extends AbstractQueryRewriterTestBase {
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
        final SqlDialect dialect = new ExasolSqlDialect(connectionMock, properties);
        final BaseRemoteMetadataReader metadataReader = new BaseRemoteMetadataReader(connectionMock, properties);
        final QueryRewriter queryRewriter = new OracleQueryRewriter(dialect, metadataReader, connectionMock);
        assertThat(queryRewriter.rewrite(this.statement, this.exaMetadata, properties),
                equalTo("IMPORT INTO (c1 DECIMAL(18, 0)) FROM JDBC AT " + CONNECTION_NAME
                        + " STATEMENT 'SELECT 1 FROM \"DUAL\"'"));
    }

    @Test
    void testRewriteToImportFromOraWithConnectionDetailsInProperties()
            throws AdapterException, SQLException, ExaConnectionAccessException {
        setImportFromOraProperty();
        this.rawProperties.put(CONNECTION_STRING_PROPERTY, "irrelevant");
        this.rawProperties.put(USERNAME_PROPERTY, "alibaba");
        this.rawProperties.put(PASSWORD_PROPERTY, "open sesame");
        this.rawProperties.put(ORACLE_CONNECTION_NAME_PROPERTY, "ora_connection");
        final AdapterProperties properties = new AdapterProperties(this.rawProperties);
        final SqlDialect dialect = new ExasolSqlDialect(null, properties);
        final QueryRewriter queryRewriter = new OracleQueryRewriter(dialect, null, null);
        assertThat(queryRewriter.rewrite(this.statement, this.exaMetadata, properties),
                equalTo("IMPORT FROM ORA AT ora_connection USER 'alibaba' IDENTIFIED BY 'open sesame'"
                        + " STATEMENT 'SELECT 1 FROM \"DUAL\"'"));
    }

    private void setImportFromOraProperty() {
        this.rawProperties.put(ORACLE_IMPORT_PROPERTY, "true");
    }

    @Test
    void testConnectionDefinitionBuilderClass() {
        final SqlDialect dialect = new OracleSqlDialect(null, AdapterProperties.emptyProperties());
        final QueryRewriter queryRewriter = new OracleQueryRewriter(dialect, null, null);
        assertThat(getMethodReturnViaReflection(queryRewriter, "createConnectionDefinitionBuilder"),
                instanceOf(OracleConnectionDefinitionBuilder.class));
    }
}