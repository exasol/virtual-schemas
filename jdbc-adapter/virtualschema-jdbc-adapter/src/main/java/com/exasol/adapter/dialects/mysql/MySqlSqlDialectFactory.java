package com.exasol.adapter.dialects.mysql;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlDialectFactory;

import java.sql.Connection;

/**
 * Factory for the MySql SQL dialect.
 */
public class MySqlSqlDialectFactory implements SqlDialectFactory {
    @Override
    public String getSqlDialectName() {
        return MySqlSqlDialect.NAME;
    }

    @Override
    public SqlDialect createSqlDialect(final Connection connection, final AdapterProperties properties) {
        return new MySqlSqlDialect(connection, properties);
    }
}
