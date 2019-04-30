package com.exasol.adapter.dialects;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.util.Collections;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.BaseRemoteMetadataReader;

@ExtendWith(MockitoExtension.class)
class SqlDialectFactoryTest {
    private static final String DIALECT_NAME = "dummy dialect";
    private SqlDialectRegistry sqlDialectRegistry;
    private final AdapterProperties dummyProperties = new AdapterProperties(Collections.emptyMap());
    private SqlDialectFactory sqlDialectFactory;
    @Mock
    private Connection mockConnection;

    @BeforeEach
    void beforeEach() {
        this.sqlDialectRegistry = SqlDialectRegistry.getInstance();
        this.sqlDialectRegistry.registerDialect(DummySqlDialect.class.getName());
        this.sqlDialectFactory = new SqlDialectFactory(this.mockConnection, this.sqlDialectRegistry,
                this.dummyProperties);
    }

    @AfterAll
    static void afterAll() {
        SqlDialectRegistry.deleteInstance();
    }

    @Test
    void testCreateSqlDialect() {
        assertThat(this.sqlDialectFactory.createSqlDialect(DIALECT_NAME), instanceOf(DummySqlDialect.class));
    }

    @Test
    void testInjectConstructorParameters() {
        final SqlDialect dialect = this.sqlDialectFactory.createSqlDialect(DIALECT_NAME);
        assertAll(() -> assertThat(dialect, instanceOf(DummySqlDialect.class)),
                () -> assertThat(((DummySqlDialect) dialect).getProperties(), sameInstance(this.dummyProperties)),
                () -> assertThat(((DummySqlDialect) dialect).getRemoteMetadataReader(),
                        instanceOf(BaseRemoteMetadataReader.class)));
    }
}