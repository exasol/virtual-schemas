package com.exasol.adapter.dialects.bigquery;

import static com.exasol.adapter.dialects.DialectTestData.getClicksTableMetadata;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.Connection;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.sql.*;
import com.google.common.collect.ImmutableList;

@ExtendWith(MockitoExtension.class)
class BigQuerySqlGenerationVisitorTest {
    @Mock
    Connection connection;

    @Test
    void visit() throws AdapterException {
        final SqlDialect dialect = new BigQuerySqlDialect(this.connection, AdapterProperties.emptyProperties());
        final SqlNodeVisitor<String> visitor = new BigQuerySqlGenerationVisitor(dialect,
                new SqlGenerationContext("catalog", "schema", false));
        final TableMetadata clicksMeta = getClicksTableMetadata();
        final SqlOrderBy orderBy = new SqlOrderBy(ImmutableList.of(new SqlColumn(0, clicksMeta.getColumns().get(0))),
                ImmutableList.of(true), ImmutableList.of(true));
        assertThat(visitor.visit(orderBy), equalTo("ORDER BY `USER_ID`"));
    }
}