package com.exasol.adapter.dialects.sybase;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlDialectFactory;

/**
 * Factory for the Sybase dialect.
 */
public class SybaseSqlDialectFactory implements SqlDialectFactory {
    @Override
    public String getSqlDialectName() {
        return SybaseSqlDialect.NAME;
    }

    @Override
    public SqlDialect createSqlDialect(final Connection connection, final AdapterProperties properties) {
        return new SybaseSqlDialect(connection, properties);
    }
}
