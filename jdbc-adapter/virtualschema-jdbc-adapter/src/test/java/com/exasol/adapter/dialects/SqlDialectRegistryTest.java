package com.exasol.adapter.dialects;

import com.exasol.adapter.dialects.impl.DB2SqlDialect;
import com.exasol.adapter.dialects.impl.ExasolSqlDialect;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SqlDialectRegistryTest {
    @BeforeEach
    void before() {
        SqlDialectRegistry.deleteInstance();
        System.clearProperty(SqlDialectRegistry.SQL_DIALECTS_PROPERTY);
    }

    @Test
    void testGetInstance() {
        final SqlDialectRegistry dialects = SqlDialectRegistry.getInstance();
        assertThat(dialects, instanceOf(SqlDialectRegistry.class));
    }

    @Test
    void testGetInstanceTwiceYieldsSameInstance() {
        assertThat(SqlDialectRegistry.getInstance(), sameInstance(SqlDialectRegistry.getInstance()));
    }

    @Test
    void testIsSupported() {
        assertThat(SqlDialectRegistry.getInstance().isSupported(ExasolSqlDialect.getPublicName()), is(true));
    }

    @Test
    void testIsNotSupported() {
        assertThat(SqlDialectRegistry.getInstance().isSupported("Unknown Dialect"), is(false));
    }

    @Test
    void testGetDialectNames() {
        assertThat(SqlDialectRegistry.getInstance().getDialectsString(), matchesPattern(
              ".*" + DB2SqlDialect.getPublicName() + ".*,.* " + ExasolSqlDialect.getPublicName() + ".*"));
    }

    @Test
    void testGetDialectByName() {
        assertThat(SqlDialectRegistry.getInstance()
                    .getDialectInstanceForNameWithContext(ExasolSqlDialect.getPublicName(), null),
              instanceOf(ExasolSqlDialect.class));

    }

    @Test
    void testReadDialectsFromSystemProperty() {
        System.setProperty(SqlDialectRegistry.SQL_DIALECTS_PROPERTY,
              "com.exasol.adapter.dialects.impl.ExasolSqlDialect");
        assertThat(SqlDialectRegistry.getInstance().getDialectsString(), equalTo("EXASOL"));
    }

    @Test
    void testUsingDialectWithoutNameMethodThrowsException() {
        System.setProperty(SqlDialectRegistry.SQL_DIALECTS_PROPERTY,
              "com.exasol.adapter.dialects.impl.DummyDialectWithoutNameMethod");
        assertThrows(SqlDialectsRegistryException.class, () -> SqlDialectRegistry.getInstance().getDialectsString());
    }

    @Test
    void testRegisteringNonExistentDialectThrowsException() {
        System.setProperty(SqlDialectRegistry.SQL_DIALECTS_PROPERTY, "this.dialect.does.not.exist.DummySqlDialect");
        assertThrows(SqlDialectsRegistryException.class, SqlDialectRegistry::getInstance);
    }

    @Test
    void testRequestingInstanceOfNonExistentDialectThrowsException() {
        assertThrows(SqlDialectsRegistryException.class,
              () -> SqlDialectRegistry.getInstance().getDialectInstanceForNameWithContext("NonExistentDialect", null));
    }

    @Test
    void testGetSqlDialectClassForName() {
        assertThat(SqlDialectRegistry.getInstance().getSqlDialectClassForName("exasol"),
              equalTo(ExasolSqlDialect.class));
    }

    @Test
    void testGetSqlDialectClassForNameThrowsException() {
        assertThrows(IllegalArgumentException.class,
              () -> SqlDialectRegistry.getInstance().getSqlDialectClassForName("exsol"));
    }
}