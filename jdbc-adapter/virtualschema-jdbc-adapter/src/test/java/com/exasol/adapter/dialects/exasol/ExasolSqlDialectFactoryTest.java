package com.exasol.adapter.dialects.exasol;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;

public class ExasolSqlDialectFactoryTest {
    private ExasolSqlDialectFactory factory;

    @BeforeEach
    void beforeEach() {
        this.factory = new ExasolSqlDialectFactory();
    }

    @Test
    void testGetName() {
        assertThat(this.factory.getSqlDialectName(), equalTo("EXASOL"));
    }

    @Test
    void testCreateDialect() {
        assertThat(this.factory.createSqlDialect(null, AdapterProperties.emptyProperties()),
                instanceOf(ExasolSqlDialect.class));
    }
}