package com.exasol.adapter.dialects.oracle;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;

public class OracleSqlDialectFactoryTest {
    private OracleSqlDialectFactory factory;

    @BeforeEach
    void beforeEach() {
        this.factory = new OracleSqlDialectFactory();
    }

    @Test
    void testGetName() {
        assertThat(this.factory.getSqlDialectName(), equalTo("ORACLE"));
    }

    @Test
    void testCreateDialect() {
        assertThat(this.factory.createSqlDialect(null, AdapterProperties.emptyProperties()),
                instanceOf(OracleSqlDialect.class));
    }
}