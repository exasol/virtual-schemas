package com.exasol.adapter.dialects;

import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.Matchers.equalTo;
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
        System.clearProperty(SqlDialects.SQL_DIALECTS_PROPERTY);
    }

    @Test
    public void testGetInstance() {
        final SqlDialects dialects = SqlDialects.getInstance();
        assertThat(dialects, instanceOf(SqlDialects.class));
    }

    @Test
    public void testGetInstanceTwiceYieldsSameInstance() {
        assertThat(SqlDialects.getInstance(), sameInstance(SqlDialects.getInstance()));
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

    @Test
    public void testReadDialectsFromSystemProperty() {
        System.setProperty(SqlDialects.SQL_DIALECTS_PROPERTY, "com.exasol.adapter.dialects.impl.ExasolSqlDialect");
        assertThat(SqlDialects.getInstance().getDialectsString(), equalTo("EXASOL"));
    }

    @Test(expected = SqlDialectsRegistryException.class)
    public void testUsingDialectWithoutNameMethodThrowsException() {
        System.setProperty(SqlDialects.SQL_DIALECTS_PROPERTY,
                "com.exasol.adapter.dialects.impl.DummyDialectWithoutNameMethod");
        SqlDialects.getInstance().getDialectsString();
    }

    @Test(expected = SqlDialectsRegistryException.class)
    public void testRegisteringNonExistentDialectThrowsException() {
        System.setProperty(SqlDialects.SQL_DIALECTS_PROPERTY, "this.dialect.does.not.exist.DummySqlDialect");
        SqlDialects.getInstance();
    }

    @Test(expected = SqlDialectsRegistryException.class)
    public void testRequestingInstanceOfNonExistentDialectThrowsException() {
        SqlDialects.getInstance();
        SqlDialects.getInstance().getDialectInstanceForNameWithContext("NonExistentDialect", null);
    }
}