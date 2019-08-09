package com.exasol.adapter.dialects.dummy;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlDialectFactory;

public class DummySqlDialectFactory implements SqlDialectFactory {
    @Override
    public String getSqlDialectName() {
        return DummySqlDialect.NAME;
    }

    @Override
    public SqlDialect createSqlDialect(final Connection connection, final AdapterProperties properties) {
        return new DummySqlDialect(connection, properties);
    }
}