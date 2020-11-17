package com.exasol.adapter.dialects.impala;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlGenerationContext;
import com.exasol.adapter.sql.SqlFunctionAggregateGroupConcat;
import com.exasol.adapter.sql.SqlLiteralString;

class ImpalaSqlGenerationVisitorTest {
    private ImpalaSqlGenerationVisitor visitor;

    @BeforeEach
    void beforeEach() {
        final SqlDialect dialect = new ImpalaSqlDialectFactory().createSqlDialect(null,
                AdapterProperties.emptyProperties());
        final SqlGenerationContext context = new SqlGenerationContext("test_catalog", "test_schema", false);
        this.visitor = new ImpalaSqlGenerationVisitor(dialect, context);
    }

    @Test
    void visitSqlFunctionAggregateGroupConcat() throws AdapterException {
        final SqlLiteralString argument = new SqlLiteralString("value'");
        final SqlFunctionAggregateGroupConcat aggregateGroupConcat = SqlFunctionAggregateGroupConcat.builder(argument)
                .separator(new SqlLiteralString("'")).build();
        assertThat(this.visitor.visit(aggregateGroupConcat),
                equalTo("GROUP_CONCAT(CAST('value\\'' AS STRING), '\\'')"));
    }
}