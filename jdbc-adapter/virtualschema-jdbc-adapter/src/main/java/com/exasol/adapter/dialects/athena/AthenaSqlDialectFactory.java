package com.exasol.adapter.dialects.athena;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlDialectFactory;

/**
 * Factory for the Athena SQL dialect
 */
public class AthenaSqlDialectFactory implements SqlDialectFactory {
    @Override
    public String getSqlDialectName() {
        return AthenaSqlDialect.NAME;
    }

    @Override
    public SqlDialect createSqlDialect(final Connection connection, final AdapterProperties properties) {
        return new AthenaSqlDialect(connection, properties);
    }
}