package com.exasol.adapter.dialects.athena;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;

public class AthenaSqlDialectFactoryTest {
    private AthenaSqlDialectFactory factory;

    @BeforeEach
    void beforeEach() {
        this.factory = new AthenaSqlDialectFactory();
    }

    @Test
    void testGetName() {
        assertThat(this.factory.getSqlDialectName(), equalTo("ATHENA"));
    }

    @Test
    void testCreateDialect() {
        assertThat(this.factory.createSqlDialect(null, AdapterProperties.emptyProperties()),
                instanceOf(AthenaSqlDialect.class));
    }
}