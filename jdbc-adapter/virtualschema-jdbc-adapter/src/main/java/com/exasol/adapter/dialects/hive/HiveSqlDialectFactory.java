package com.exasol.adapter.dialects.hive;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlDialectFactory;

/**
 * Factory for the Hive SQL dialect
 */
public class HiveSqlDialectFactory implements SqlDialectFactory {
    @Override
    public String getSqlDialectName() {
        return HiveSqlDialect.NAME;
    }

    @Override
    public SqlDialect createSqlDialect(final Connection connection, final AdapterProperties properties) {
        return new HiveSqlDialect(connection, properties);
    }
}