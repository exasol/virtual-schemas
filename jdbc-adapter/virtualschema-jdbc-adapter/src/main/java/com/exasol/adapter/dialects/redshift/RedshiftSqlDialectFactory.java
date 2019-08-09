package com.exasol.adapter.dialects.redshift;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlDialectFactory;

/**
 * Factory for the Redshift SQL dialect.
 */
public class RedshiftSqlDialectFactory implements SqlDialectFactory {
    @Override
    public String getSqlDialectName() {
        return RedshiftSqlDialect.NAME;
    }

    @Override
    public SqlDialect createSqlDialect(final Connection connection, final AdapterProperties properties) {
        return new RedshiftSqlDialect(connection, properties);
    }
}
