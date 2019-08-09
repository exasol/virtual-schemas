package com.exasol.adapter.dialects.exasol;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlDialectFactory;

/**
 * Factory for the Exasol SQL dialect.
 */
public class ExasolSqlDialectFactory implements SqlDialectFactory {
    @Override
    public String getSqlDialectName() {
        return ExasolSqlDialect.NAME;
    }

    @Override
    public SqlDialect createSqlDialect(final Connection connection, final AdapterProperties properties) {
        return new ExasolSqlDialect(connection, properties);
    }
}
