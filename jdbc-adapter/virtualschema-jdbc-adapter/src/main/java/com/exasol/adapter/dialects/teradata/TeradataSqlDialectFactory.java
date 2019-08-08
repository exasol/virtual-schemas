package com.exasol.adapter.dialects.teradata;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlDialectFactory;

/**
 * Factory for the Teradata dialect.
 */
public class TeradataSqlDialectFactory implements SqlDialectFactory {
    @Override
    public String getSqlDialectName() {
        return TeradataSqlDialect.NAME;
    }

    @Override
    public SqlDialect createSqlDialect(final Connection connection, final AdapterProperties properties) {
        return new TeradataSqlDialect(connection, properties);
    }
}
