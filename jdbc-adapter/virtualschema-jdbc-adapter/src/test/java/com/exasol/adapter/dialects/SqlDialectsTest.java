package com.exasol.adapter.dialects;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.HashSet;

import org.junit.Before;
import org.junit.Test;

import com.exasol.adapter.dialects.impl.AnotherDummySqlDialect;
import com.exasol.adapter.dialects.impl.DummySqlDialect;

public class SqlDialectsTest {
    private SqlDialects dialects;

    @Before
    public void before() {
        this.dialects = SqlDialects.getInstance();
    }

    @Test
    public void testGetInstance() {
        assertThat(this.dialects, instanceOf(SqlDialects.class));
    }

    @Test
    public void testIsSupported() {
        this.dialects.register(DummySqlDialect.class);
        assertThat(this.dialects.isSupported(DummySqlDialect.getPublicName()), is(true));
    }

    @Test
    public void testGetDialectNames() {
        this.dialects.register(DummySqlDialect.class);
        this.dialects.register(AnotherDummySqlDialect.class);
        assertThat(this.dialects.getDialectsString(),
                equalTo(AnotherDummySqlDialect.getPublicName() + ", " + DummySqlDialect.getPublicName()));
    }

    @Test
    public void testGetDialectByName() {
        this.dialects.register(DummySqlDialect.class);
        assertThat(this.dialects.getDialectInstanceForNameWithContext(DummySqlDialect.getPublicName(), null),
                instanceOf(DummySqlDialect.class));

    }

    @Test
    public void testRegisterMultipleDialects() {
        this.dialects.registerAll(new HashSet<Class<? extends SqlDialect>>(
                Arrays.asList(DummySqlDialect.class, AnotherDummySqlDialect.class)));
        assertThat(this.dialects.getDialectsString(),
                equalTo(AnotherDummySqlDialect.getPublicName() + ", " + DummySqlDialect.getPublicName()));
    }
}
