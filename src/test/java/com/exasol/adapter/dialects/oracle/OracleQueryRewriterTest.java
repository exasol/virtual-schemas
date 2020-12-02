package com.exasol.adapter.dialects.oracle;

import static com.exasol.adapter.dialects.oracle.OracleProperties.ORACLE_CONNECTION_NAME_PROPERTY;
import static com.exasol.adapter.dialects.oracle.OracleProperties.ORACLE_IMPORT_PROPERTY;
import static com.exasol.reflect.ReflectionUtils.getMethodReturnViaReflection;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

import java.sql.SQLException;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.ConnectionFactory;
import com.exasol.adapter.sql.TestSqlStatementFactory;

@ExtendWith(MockitoExtension.class)
public class OracleQueryRewriterTest extends AbstractQueryRewriterTestBase {
    @BeforeEach
    void beforeEach() {
        this.statement = TestSqlStatementFactory.createSelectOneFromDual();
    }

    @Test
    void testRewriteToImportFromOraWithConnectionDetailsInProperties(
            @Mock final ConnectionFactory connectionFactoryMock) throws AdapterException, SQLException {
        final AdapterProperties properties = new AdapterProperties(Map.of( //
                ORACLE_IMPORT_PROPERTY, "true", //
                ORACLE_CONNECTION_NAME_PROPERTY, "ora_connection"));
        final SqlDialectFactory dialectFactory = new OracleSqlDialectFactory();
        final SqlDialect dialect = dialectFactory.createSqlDialect(connectionFactoryMock, properties);
        final QueryRewriter queryRewriter = new OracleQueryRewriter(dialect, null, null);
        assertThat(queryRewriter.rewrite(this.statement, EXA_METADATA, properties),
                equalTo("IMPORT FROM ORA AT ora_connection STATEMENT 'SELECT TO_CHAR(1) FROM \"DUAL\"'"));
    }

    @Test
    void testConnectionDefinitionBuilderClass() {
        final SqlDialect dialect = new OracleSqlDialect(null, AdapterProperties.emptyProperties());
        final QueryRewriter queryRewriter = new OracleQueryRewriter(dialect, null, null);
        assertThat(getMethodReturnViaReflection(queryRewriter, "createConnectionDefinitionBuilder"),
                instanceOf(OracleConnectionDefinitionBuilder.class));
    }
}