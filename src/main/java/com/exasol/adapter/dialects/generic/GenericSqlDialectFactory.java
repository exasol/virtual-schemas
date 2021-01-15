package com.exasol.adapter.dialects.generic;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlDialectFactory;
import com.exasol.adapter.jdbc.ConnectionFactory;
import com.exasol.logging.VersionCollector;

/**
 * Factory for the Generic SQL dialect.
 */
public class GenericSqlDialectFactory implements SqlDialectFactory {
    @Override
    public String getSqlDialectName() {
        return GenericSqlDialect.NAME;
    }

    @Override
    public SqlDialect createSqlDialect(final ConnectionFactory connectionFactory, final AdapterProperties properties) {
        return new GenericSqlDialect(connectionFactory, properties);
    }

    @Override
    public String getSqlDialectVersion() {
        final VersionCollector versionCollector = new VersionCollector(
                "META-INF/maven/com.exasol/virtual-schema-jdbc-adapter/pom.properties");
        return versionCollector.getVersionNumber();
    }
}