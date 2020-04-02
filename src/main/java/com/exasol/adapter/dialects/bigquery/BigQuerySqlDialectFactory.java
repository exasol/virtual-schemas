package com.exasol.adapter.dialects.bigquery;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.AbstractSqlDialectFactory;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.jdbc.ConnectionFactory;

/**
 * Factory for the BigQuery SQL dialect.
 */
public class BigQuerySqlDialectFactory extends AbstractSqlDialectFactory {
    @Override
    public String getSqlDialectName() {
        return BigQuerySqlDialect.NAME;
    }

    @Override
    public SqlDialect createSqlDialect(final ConnectionFactory connectionFactory, final AdapterProperties properties) {
        return new BigQuerySqlDialect(connectionFactory, properties);
    }
}