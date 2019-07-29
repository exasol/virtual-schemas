package com.exasol.adapter.dialects;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.sql.Connection;
import java.util.Collections;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.dummy.DummySqlDialect;
import com.exasol.adapter.dialects.dummy.DummySqlDialectFactory;
import com.exasol.adapter.jdbc.BaseRemoteMetadataReader;

@ExtendWith(MockitoExtension.class)
class AbstractSqlDialectFactoryTest {
    private static final String DIALECT_NAME = "dummy dialect";
    private static final AdapterProperties DUMMY_PROPERTIES = new AdapterProperties(Collections.emptyMap());
    private SqlDialectFactory sqlDialectFactory;
    @Mock
    private Connection connectionMock;

    @BeforeEach
    void beforeEach() {
        this.sqlDialectFactory = new DummySqlDialectFactory();
    }

    @AfterAll
    static void afterAll() {
        SqlDialectRegistry.deleteInstance();
    }

    @Test
    void testGetSqlDialecName() {
        assertThat(this.sqlDialectFactory.getSqlDialectName(), equalTo(DIALECT_NAME));
    }

    @Test
    void testCreateSqlDialect() {
        final SqlDialect dialect = this.sqlDialectFactory.createSqlDialect(this.connectionMock, DUMMY_PROPERTIES);
        assertAll(() -> assertThat(dialect, instanceOf(DummySqlDialect.class)),
                () -> assertThat(((DummySqlDialect) dialect).getProperties(), sameInstance(DUMMY_PROPERTIES)),
                () -> assertThat(((DummySqlDialect) dialect).getRemoteMetadataReader(),
                        instanceOf(BaseRemoteMetadataReader.class)));
    }
}