package com.exasol.adapter.dialects;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.junit.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
class SqlDialectFactoryTest {
    private static final String DIALECT_NAME = "dummy dialect";
    @Mock
    private Connection mockConnection;
    private SqlDialect dummySqlDialect;
    private SqlDialectRegistry sqlDialectRegistry;
    private final Map<String, String> dummyProperties = new HashMap<>();
    private SqlDialectFactory sqlDialectFactory;

    @BeforeEach
    void beforeEach() {
        this.sqlDialectFactory = new SqlDialectFactory(this.mockConnection, this.sqlDialectRegistry, this.dummyProperties);
        this.dummySqlDialect = new DummySqlDialect();
        this.sqlDialectRegistry = SqlDialectRegistry.getInstance();
        this.sqlDialectRegistry.registerDialect(this.dummySqlDialect.getClass().getName());
    }

    @Test
    void createSqlDialect() {
        assertThat(this.sqlDialectFactory.createSqlDialect(DIALECT_NAME), instanceOf(DummySqlDialect.class));
    }

    @Test
    void testInjectConstructorParameters() {

    }
}
