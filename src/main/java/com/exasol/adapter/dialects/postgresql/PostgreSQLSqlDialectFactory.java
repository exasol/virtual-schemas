package com.exasol.adapter.dialects.postgresql;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.AbstractSqlDialectFactory;
import com.exasol.adapter.dialects.SqlDialect;

/**
 * Factory for the PostgreSQL SQL dialect.
 */
public class PostgreSQLSqlDialectFactory extends AbstractSqlDialectFactory {
    @Override
    public String getSqlDialectName() {
        return PostgreSQLSqlDialect.NAME;
    }

    @Override
    public SqlDialect createSqlDialect(final Connection connection, final AdapterProperties properties) {
        return new PostgreSQLSqlDialect(connection, properties);
    }
}