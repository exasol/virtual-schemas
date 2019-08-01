package com.exasol.adapter.dialects.sqlserver;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlDialectFactory;

/**
 * Factory for the SQL Server dialect
 */
public class SqlServerSqlDialectFactory implements SqlDialectFactory {
    @Override
    public String getSqlDialectName() {
        return SqlServerSqlDialect.NAME;
    }

    @Override
    public SqlDialect createSqlDialect(final Connection connection, final AdapterProperties properties) {
        return new SqlServerSqlDialect(connection, properties);
    }
}