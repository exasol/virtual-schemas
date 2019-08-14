package com.exasol.adapter.dialects.db2;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlDialectFactory;

/**
 * Factory for the DB2 SQL dialect.
 */
public class DB2SqlDialectFactory implements SqlDialectFactory {
    @Override
    public String getSqlDialectName() {
        return DB2SqlDialect.NAME;
    }

    @Override
    public SqlDialect createSqlDialect(final Connection connection, final AdapterProperties properties) {
        return new DB2SqlDialect(connection, properties);
    }
}
