package com.exasol.adapter.dialects.impala;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;

public class ImpalaSqlDialectFactoryTest {
    private ImpalaSqlDialectFactory factory;

    @BeforeEach
    void beforeEach() {
        this.factory = new ImpalaSqlDialectFactory();
    }

    @Test
    void testGetName() {
        assertThat(this.factory.getSqlDialectName(), equalTo("IMPALA"));
    }

    @Test
    void testCreateDialect() {
        assertThat(this.factory.createSqlDialect(null, AdapterProperties.emptyProperties()),
                instanceOf(ImpalaSqlDialect.class));
    }
}