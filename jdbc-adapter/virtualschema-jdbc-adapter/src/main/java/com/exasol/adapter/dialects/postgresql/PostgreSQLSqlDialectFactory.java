package com.exasol.adapter.dialects.postgresql;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlDialectFactory;

/**
 * Factory for the PostgreSQL SQL dialect
 */
public class PostgreSQLSqlDialectFactory implements SqlDialectFactory {
    @Override
    public String getSqlDialectName() {
        return PostgreSQLSqlDialect.NAME;
    }

    @Override
    public SqlDialect createSqlDialect(final Connection connection, final AdapterProperties properties) {
        return new PostgreSQLSqlDialect(connection, properties);
    }
}