package com.exasol.adapter.dialects.db2;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;

class DB2SqlDialectFactoryTest {
    private DB2SqlDialectFactory factory;

    @BeforeEach
    void beforeEach() {
        this.factory = new DB2SqlDialectFactory();
    }

    @Test
    void testGetName() {
        assertThat(this.factory.getSqlDialectName(), equalTo("DB2"));
    }

    @Test
    void testCreateDialect() {
        assertThat(this.factory.createSqlDialect(null, AdapterProperties.emptyProperties()),
                instanceOf(DB2SqlDialect.class));
    }
}