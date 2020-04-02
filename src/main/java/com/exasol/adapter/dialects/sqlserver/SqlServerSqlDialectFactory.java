package com.exasol.adapter.dialects.sqlserver;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.AbstractSqlDialectFactory;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.jdbc.ConnectionFactory;

/**
 * Factory for the SQL Server dialect.
 */
public class SqlServerSqlDialectFactory extends AbstractSqlDialectFactory {
    @Override
    public String getSqlDialectName() {
        return SqlServerSqlDialect.NAME;
    }

    @Override
    public SqlDialect createSqlDialect(final ConnectionFactory connectionFactory, final AdapterProperties properties) {
        return new SqlServerSqlDialect(connectionFactory, properties);
    }
}