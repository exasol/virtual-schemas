package com.exasol.adapter.dialects;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Before;
import org.junit.Test;

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
    public void getDialectByName() {
        this.dialects.register(DummySqlDialect.class);
        assertThat(this.dialects.getDialectInstanceForNameWithContext(DummySqlDialect.getPublicName(), null),
                instanceOf(DummySqlDialect.class));

    }
}
