package com.exasol.adapter.dialects.generic;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlDialectFactory;

/**
 * Factory for the Generic SQL dialect.
 */
public class GenericSqlDialectFactory implements SqlDialectFactory {
    @Override
    public String getSqlDialectName() {
        return GenericSqlDialect.NAME;
    }

    @Override
    public SqlDialect createSqlDialect(final Connection connection, final AdapterProperties properties) {
        return new GenericSqlDialect(connection, properties);
    }
}
