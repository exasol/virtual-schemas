package com.exasol.adapter.jdbc;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterFactory;

class JdbcAdapterFactoryTest {
    @Test
    void getAdapterName() {
        AdapterFactory factory = new JdbcAdapterFactory();
        assertThat(factory.getAdapterName(), equalTo("JDBC Adapter"));
    }
}