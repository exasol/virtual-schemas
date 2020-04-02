package com.exasol.adapter.dialects.dummy;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.AbstractSqlDialectFactory;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.jdbc.ConnectionFactory;

public class DummySqlDialectFactory extends AbstractSqlDialectFactory {
    @Override
    public String getSqlDialectName() {
        return DummySqlDialect.NAME;
    }

    @Override
    public SqlDialect createSqlDialect(final ConnectionFactory connectionFactory, final AdapterProperties properties) {
        return new DummySqlDialect(connectionFactory, properties);
    }
}