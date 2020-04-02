package com.exasol.adapter.dialects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.dummy.DummySqlDialect;
import com.exasol.adapter.dialects.dummy.DummySqlDialectFactory;
import com.exasol.adapter.jdbc.ConnectionFactory;

@ExtendWith(MockitoExtension.class)
class AbstractSqlDialectFactoryTest {
    private static final AdapterProperties DUMMY_PROPERTIES = AdapterProperties.emptyProperties();
    private SqlDialectFactory sqlDialectFactory;

    @BeforeEach
    void beforeEach() {
        this.sqlDialectFactory = new DummySqlDialectFactory();
    }

    @Test
    void testGetSqlDialecName() {
        assertThat(this.sqlDialectFactory.getSqlDialectName(), equalTo("DUMMYDIALECT"));
    }

    @Test
    void testCreateSqlDialect(@Mock final ConnectionFactory connectionFactoryMock) {
        final SqlDialect dialect = this.sqlDialectFactory.createSqlDialect(connectionFactoryMock, DUMMY_PROPERTIES);
        assertAll(() -> assertThat(dialect, instanceOf(DummySqlDialect.class)),
                () -> assertThat(((DummySqlDialect) dialect).getProperties(), sameInstance(DUMMY_PROPERTIES)));
    }
}