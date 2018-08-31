package com.exasol.adapter.dialects.impl;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.exasol.adapter.dialects.SqlDialects;

public class ExasolSqlDialectRegistrationTest {
    @Test
    public void testRegistryContainsDialect() {
        final SqlDialects dialects = SqlDialects.getInstance();
        assertThat(dialects.isSupported(ExasolSqlDialect.NAME), is(true));
    }
}
