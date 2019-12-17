package com.exasol.adapter.dialects.mysql;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.AbstractSqlDialectFactory;
import com.exasol.adapter.dialects.SqlDialect;

/**
 * Factory for the MySql SQL dialect.
 */
public class MySqlSqlDialectFactory extends AbstractSqlDialectFactory {
    @Override
    public SqlDialect createSqlDialect(final Connection connection, final AdapterProperties properties) {
        return new MySqlSqlDialect(connection, properties);
    }

    @Override
    public String getSqlDialectName() {
        return MySqlSqlDialect.NAME;
    }
}