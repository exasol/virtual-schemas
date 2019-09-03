package com.exasol.adapter.dialects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.sql.Connection;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.dummy.DummySqlDialect;
import com.exasol.adapter.dialects.dummy.DummySqlDialectFactory;
import com.exasol.adapter.jdbc.BaseRemoteMetadataReader;

@ExtendWith(MockitoExtension.class)
class AbstractSqlDialectFactoryTest {
    private static final AdapterProperties DUMMY_PROPERTIES = AdapterProperties.emptyProperties();
    private SqlDialectFactory sqlDialectFactory;
    @Mock
    private Connection connectionMock;

    @BeforeEach
    void beforeEach() {
        this.sqlDialectFactory = new DummySqlDialectFactory();
    }

    @Test
    void testGetSqlDialecName() {
        assertThat(this.sqlDialectFactory.getSqlDialectName(), equalTo("DUMMYDIALECT"));
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