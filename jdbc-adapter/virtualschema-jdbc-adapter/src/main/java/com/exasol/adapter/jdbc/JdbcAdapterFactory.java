package com.exasol.adapter.jdbc;

import java.util.*;

import com.exasol.adapter.*;
import com.exasol.logging.*;

/**
 * This class implements a factory for the {@link JdbcAdapter}.
 * <p>
 * Note that this class must be registered in a resource file called
 * <code>META-INF/services/com.exasol.adapter.AdapterFactory</code> in order for the {@link ServiceLoader} to find it.
 */
public class JdbcAdapterFactory implements AdapterFactory {
    private static final String ADAPTER_NAME = "JDBC Adapter";

    @Override
    public Set<String> getSupportedAdapterNames() {
        return new HashSet<>(Arrays.asList("ATHENA", "BIGQUERY", "DB2", "EXASOL", "GENERIC", "HIVE", "IMPALA", "ORACLE",
                "POSTGRESQL", "REDSHIFT", "SAPHANA", "SQLSERVER", "SYBASE", "TERADATA"));
    }

    @Override
    public VirtualSchemaAdapter createAdapter() {
        return new JdbcAdapter();
    }

    @Override
    public String getAdapterVersion() {
        final VersionCollector versionCollector = new VersionCollector(
                "META-INF/maven/com.exasol/virtualschema-jdbc-adapter/pom.properties");
        return versionCollector.getVersionNumber();
    }

    @Override
    public String getAdapterName() {
        return ADAPTER_NAME;
    }
}