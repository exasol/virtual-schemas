package com.exasol.adapter.dialects.snowflake;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;

public class SnowflakeSqlDialectFactoryTest {
    private SnowflakeSqlDialectFactory factory;

    @BeforeEach
    void beforeEach() {
        this.factory = new SnowflakeSqlDialectFactory();
    }

    @Test
    void testGetName() {
        assertThat(this.factory.getSqlDialectName(), equalTo("SNOWFLAKE"));
    }

    @Test
    void testCreateDialect() {
        assertThat(this.factory.createSqlDialect(null, AdapterProperties.emptyProperties()),
                instanceOf(SnowflakeSqlDialect.class));
    }
}
