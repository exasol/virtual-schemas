package com.exasol.adapter.dialects.bigquery;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlDialectFactory;

/**
 * Factory for the BigQuery SQL dialect.
 */
public class BigQuerySqlDialectFactory implements SqlDialectFactory {
    @Override
    public String getSqlDialectName() {
        return BigQuerySqlDialect.NAME;
    }

    @Override
    public SqlDialect createSqlDialect(final Connection connection, final AdapterProperties properties) {
        return new BigQuerySqlDialect(connection, properties);
    }
}
