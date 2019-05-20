package com.exasol.adapter.jdbc;

import java.util.*;

import com.exasol.adapter.AdapterFactory;
import com.exasol.adapter.VirtualSchemaAdapter;

/**
 * This class implements a factory for the {@link JdbcAdapter}.
 * <p>
 * Note that this class must be registered in a resource file called
 * <code>META-INF/services/com.exasol.adapter.AdapterFactory</code> in order for the {@link ServiceLoader} to find it.
 */
public class JdbcAdapterFactory implements AdapterFactory {
    @Override
    public Set<String> getSupportedAdapterNames() {
        return new HashSet<>(Arrays.asList("ATHENA", "BIG QUERY", "DB2", "EXASOL", "GENERIC", "HIVE", "IMPALA", "ORACLE",
                "POSTGRESQL", "REDSHIFT", "SQLSERVER", "SYBASE", "TERADATA"));
    }

    @Override
    public VirtualSchemaAdapter createAdapter() {
        return new JdbcAdapter();
    }
}