package com.exasol.adapter.dialects.sybase;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.AbstractSqlDialectFactory;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.jdbc.ConnectionFactory;

/**
 * Factory for the Sybase dialect.
 */
public class SybaseSqlDialectFactory extends AbstractSqlDialectFactory {
    @Override
    public String getSqlDialectName() {
        return SybaseSqlDialect.NAME;
    }

    @Override
    public SqlDialect createSqlDialect(final ConnectionFactory connectionFactory, final AdapterProperties properties) {
        return new SybaseSqlDialect(connectionFactory, properties);
    }
}