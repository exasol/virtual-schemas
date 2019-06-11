package com.exasol.adapter.dialects.bigquery;

import com.exasol.adapter.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.metadata.*;
import com.exasol.adapter.sql.*;
import com.google.common.collect.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.mockito.*;
import org.mockito.junit.jupiter.*;

import java.sql.*;
import java.util.*;

import static com.exasol.adapter.dialects.DialectTestData.getClicksTableMetadata;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class BigQuerySqlGenerationVisitorTest {
    @Mock
    Connection connection;

    @Test
    void visit() throws AdapterException {
        final SqlDialect dialect = new BigQuerySqlDialect(this.connection, AdapterProperties.emptyProperties());
        final SqlGenerationVisitor visitor = new BigQuerySqlGenerationVisitor(dialect,
                new SqlGenerationContext("catalog", "schema", false));
        final TableMetadata clicksMeta = getClicksTableMetadata();
        final SqlOrderBy orderBy = new SqlOrderBy(ImmutableList.of(new SqlColumn(0, clicksMeta.getColumns().get(0))),
                ImmutableList.of(true), ImmutableList.of(true));
        assertThat(visitor.visit(orderBy), equalTo("ORDER BY `USER_ID`"));
    }
}