package com.exasol.adapter.dialects.oracle;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlDialectFactory;

/**
 * Factory for the Oracle SQL dialect.
 */
public class OracleSqlDialectFactory implements SqlDialectFactory {
    @Override
    public String getSqlDialectName() {
        return OracleSqlDialect.NAME;
    }

    @Override
    public SqlDialect createSqlDialect(final Connection connection, final AdapterProperties properties) {
        return new OracleSqlDialect(connection, properties);
    }
}
