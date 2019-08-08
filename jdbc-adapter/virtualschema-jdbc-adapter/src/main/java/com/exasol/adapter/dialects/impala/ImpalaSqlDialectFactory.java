package com.exasol.adapter.dialects.impala;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlDialectFactory;

/**
 * Factory for the Impala SQL dialect.
 */
public class ImpalaSqlDialectFactory implements SqlDialectFactory {
    @Override
    public String getSqlDialectName() {
        return ImpalaSqlDialect.NAME;
    }

    @Override
    public SqlDialect createSqlDialect(final Connection connection, final AdapterProperties properties) {
        return new ImpalaSqlDialect(connection, properties);
    }
}
