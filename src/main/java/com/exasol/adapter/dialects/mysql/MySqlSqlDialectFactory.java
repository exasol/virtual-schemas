package com.exasol.adapter.dialects.mysql;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.AbstractSqlDialectFactory;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.jdbc.ConnectionFactory;

/**
 * Factory for the MySql SQL dialect.
 */
public class MySqlSqlDialectFactory extends AbstractSqlDialectFactory {
    @Override
    public SqlDialect createSqlDialect(final ConnectionFactory connectionFactory, final AdapterProperties properties) {
        return new MySqlSqlDialect(connectionFactory, properties);
    }

    @Override
    public String getSqlDialectName() {
        return MySqlSqlDialect.NAME;
    }
}