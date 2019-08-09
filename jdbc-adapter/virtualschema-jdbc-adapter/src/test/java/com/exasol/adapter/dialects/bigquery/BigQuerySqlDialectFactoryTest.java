package com.exasol.adapter.dialects.bigquery;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;

public class BigQuerySqlDialectFactoryTest {
    private BigQuerySqlDialectFactory factory;

    @BeforeEach
    void beforeEach() {
        this.factory = new BigQuerySqlDialectFactory();
    }

    @Test
    void testGetName() {
        assertThat(this.factory.getSqlDialectName(), equalTo("BIGQUERY"));
    }

    @Test
    void testCreateDialect() {
        assertThat(this.factory.createSqlDialect(null, AdapterProperties.emptyProperties()),
                instanceOf(BigQuerySqlDialect.class));
    }
}