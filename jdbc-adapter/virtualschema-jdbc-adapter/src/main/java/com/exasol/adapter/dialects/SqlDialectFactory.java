package com.exasol.adapter.dialects;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;

/**
 * This is the common interface for all factories that produce SQL dialects.
 */
public interface SqlDialectFactory {
    /**
     * Create an instance of the SQL dialect adapter matching the dialect name.
     *
     * @param connection JDBC connection to the remote data source
     * @param properties user-defined adapter properties
     * @return SQL dialect adapter
     */
    public SqlDialect createSqlDialect(final Connection connection, final AdapterProperties properties);

    /**
     * Get the name of the SQL dialect this factory can produce.
     *
     * @return SQL dialect name
     */
    public String getSqlDialectName();
}
