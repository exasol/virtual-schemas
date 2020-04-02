package com.exasol.adapter.dialects.athena;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.AbstractSqlDialectFactory;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.jdbc.ConnectionFactory;

/**
 * Factory for the Athena SQL dialect.
 */
public class AthenaSqlDialectFactory extends AbstractSqlDialectFactory {
    @Override
    public String getSqlDialectName() {
        return AthenaSqlDialect.NAME;
    }

    @Override
    public SqlDialect createSqlDialect(final ConnectionFactory connectionFactory, final AdapterProperties properties) {
        return new AthenaSqlDialect(connectionFactory, properties);
    }
}