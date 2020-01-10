package com.exasol.adapter.dialects.sqlserver;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;

public class SqlServerDialectFactoryTest {
    private SqlServerSqlDialectFactory factory;

    @BeforeEach
    void beforeEach() {
        this.factory = new SqlServerSqlDialectFactory();
    }

    @Test
    void testGetName() {
        assertThat(this.factory.getSqlDialectName(), equalTo("SQLSERVER"));
    }

    @Test
    void testCreateDialect() {
        assertThat(this.factory.createSqlDialect(null, AdapterProperties.emptyProperties()),
                instanceOf(SqlServerSqlDialect.class));
    }
}