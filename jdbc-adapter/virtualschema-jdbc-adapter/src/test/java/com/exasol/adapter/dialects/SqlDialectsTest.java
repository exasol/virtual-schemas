package com.exasol.adapter.dialects;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.Assert.assertThat;

import org.junit.Before;
import org.junit.Test;

import com.exasol.adapter.dialects.impl.DB2SqlDialect;
import com.exasol.adapter.dialects.impl.ExasolSqlDialect;

public class SqlDialectsTest {
    @Before
    public void before() {
        SqlDialects.deleteInstance();
    }

    @Test
    public void testGetInstance() {
        final SqlDialects dialects = SqlDialects.getInstance();
        assertThat(dialects, instanceOf(SqlDialects.class));
    }

    @Test
    public void testIsSupported() {
        assertThat(SqlDialects.getInstance().isSupported(ExasolSqlDialect.getPublicName()), is(true));
    }

    @Test
    public void testIsNotSupported() {
        assertThat(SqlDialects.getInstance().isSupported("Unknown Dialect"), is(false));
    }

    @Test
    public void testGetDialectNames() {
        assertThat(SqlDialects.getInstance().getDialectsString(), matchesPattern(
                ".*" + DB2SqlDialect.getPublicName() + ".*,.* " + ExasolSqlDialect.getPublicName() + ".*"));
    }

    @Test
    public void testGetDialectByName() {
        assertThat(
                SqlDialects.getInstance().getDialectInstanceForNameWithContext(ExasolSqlDialect.getPublicName(), null),
                instanceOf(ExasolSqlDialect.class));

    }
}
