package com.exasol.adapter.dialects.postgresql;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;

public class PostgreSQLSqlDialectFactoryTest {
    private PostgreSQLSqlDialectFactory factory;

    @BeforeEach
    void beforeEach() {
        this.factory = new PostgreSQLSqlDialectFactory();
    }

    @Test
    void testGetName() {
        assertThat(this.factory.getSqlDialectName(), equalTo("POSTGRESQL"));
    }

    @Test
    void testCreateDialect() {
        assertThat(this.factory.createSqlDialect(null, AdapterProperties.emptyProperties()),
                instanceOf(PostgreSQLSqlDialect.class));
    }
}