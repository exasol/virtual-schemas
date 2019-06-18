package com.exasol.adapter.jdbc;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.*;

import org.junit.jupiter.api.*;

import com.exasol.adapter.*;

class JdbcAdapterFactoryTest {
    @Test
    void getAdapterName() {
        AdapterFactory factory = new JdbcAdapterFactory();
        assertThat(factory.getAdapterName(), equalTo("JDBC Adapter"));
    }
}