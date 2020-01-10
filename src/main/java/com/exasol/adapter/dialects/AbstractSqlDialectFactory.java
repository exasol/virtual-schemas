package com.exasol.adapter.dialects;

import com.exasol.logging.VersionCollector;

/**
 * Common base class for all factories of JDBC-based SQL dialect.
 */
public abstract class AbstractSqlDialectFactory implements SqlDialectFactory {
    @Override
    public String getSqlDialectVersion() {
        final VersionCollector versionCollector = new VersionCollector(
                "META-INF/maven/com.exasol/virtual-schema-jdbc-adapter/pom.properties");
        return versionCollector.getVersionNumber();
    }
}
