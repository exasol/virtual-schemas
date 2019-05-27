package com.exasol.adapter.dialects.exasol;

import static com.exasol.adapter.AdapterProperties.*;
import static com.exasol.adapter.dialects.exasol.ExasolProperties.EXASOL_IMPORT_PROPERTY;
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
import com.exasol.adapter.jdbc.BaseRemoteMetadataReader;

class ExasolQueryRewriterTest extends AbstractQueryRewriterTest {
    @BeforeEach
    void beforeEach() {
        this.exaConnectionInformation = mock(ExaConnectionInformation.class);
        this.exaMetadata = mock(ExaMetadata.class);
        this.rawProperties = new HashMap<>();
        this.statement = createSimpleSelectStatement();
    }

    @Test
    void testRewriteWithJdbcConnection() throws AdapterException, SQLException, ExaConnectionAccessException {
        mockExasolNamedConnection();
        final Connection connectionMock = mockConnection();
        setConnectionNameProperty();
        final AdapterProperties properties = new AdapterProperties(this.rawProperties);
        final SqlDialect dialect = new ExasolSqlDialect(connectionMock, properties);
        final BaseRemoteMetadataReader metadataReader = new BaseRemoteMetadataReader(connectionMock, properties);
        final QueryRewriter queryRewriter = new ExasolQueryRewriter(dialect, metadataReader, connectionMock);
        assertThat(queryRewriter.rewrite(this.statement, this.exaMetadata, properties),
                equalTo("IMPORT INTO (c1 DECIMAL(18, 0)) FROM JDBC AT " + CONNECTION_NAME
                        + " STATEMENT 'SELECT 1 FROM \"DUAL\"'"));
    }

    @Test
    void testRewriteLocal() throws AdapterException, SQLException, ExaConnectionAccessException {
        setIsLocalProperty();
        final AdapterProperties properties = new AdapterProperties(this.rawProperties);
        final SqlDialect dialect = new ExasolSqlDialect(null, properties);
        final QueryRewriter queryRewriter = new ExasolQueryRewriter(dialect, null, null);
        assertThat(queryRewriter.rewrite(this.statement, this.exaMetadata, properties),
                equalTo("SELECT 1 FROM \"DUAL\""));
    }

    private void setIsLocalProperty() {
        this.rawProperties.put(IS_LOCAL_PROPERTY, "true");
    }

    @Test
    void testRewriteToImportFromExaWithConnectionDetailsInProperties()
            throws AdapterException, SQLException, ExaConnectionAccessException {
        setImportFromExaProperty();
        this.rawProperties.put(CONNECTION_STRING_PROPERTY, "irrelevant");
        this.rawProperties.put(USERNAME_PROPERTY, "alibaba");
        this.rawProperties.put(PASSWORD_PROPERTY, "open sesame");
        this.rawProperties.put(ExasolProperties.EXASOL_CONNECTION_STRING_PROPERTY, "localhost:7861");
        final AdapterProperties properties = new AdapterProperties(this.rawProperties);
        final SqlDialect dialect = new ExasolSqlDialect(null, properties);
        final QueryRewriter queryRewriter = new ExasolQueryRewriter(dialect, null, null);
        assertThat(queryRewriter.rewrite(this.statement, this.exaMetadata, properties),
                equalTo("IMPORT FROM EXA AT 'localhost:7861' USER 'alibaba' IDENTIFIED BY 'open sesame'"
                        + " STATEMENT 'SELECT 1 FROM \"DUAL\"'"));
    }

    private void setImportFromExaProperty() {
        this.rawProperties.put(EXASOL_IMPORT_PROPERTY, "true");
    }

    @Test
    void testConnectionDefinitionBuilderClass() {
        final SqlDialect dialect = new ExasolSqlDialect(null, AdapterProperties.emptyProperties());
        final QueryRewriter queryRewriter = new ExasolQueryRewriter(dialect, null, null);
        assertThat(getMethodReturnViaReflection(queryRewriter, "createConnectionDefinitionBuilder"),
                instanceOf(ExasolConnectionDefinitionBuilder.class));
    }
}