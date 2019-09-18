package com.exasol.adapter.dialects.mysql;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlDialectFactory;

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
