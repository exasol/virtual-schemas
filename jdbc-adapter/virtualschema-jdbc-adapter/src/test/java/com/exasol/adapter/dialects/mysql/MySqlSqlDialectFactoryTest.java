package com.exasol.adapter.dialects.mysql;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;

class MySqlSqlDialectFactoryTest {

    private MySqlSqlDialectFactory factory;

    @BeforeEach
    void beforeEach() {
        this.factory = new MySqlSqlDialectFactory();
    }

    @Test
    void testGetName() {
        assertThat(this.factory.getSqlDialectName(), equalTo("MYSQL"));
    }

    @Test
    void testCreateDialect() {
        assertThat(this.factory.createSqlDialect(null, AdapterProperties.emptyProperties()),
                instanceOf(MySqlSqlDialect.class));
    }

}
