package com.exasol.adapter.dialects.redshift;

import java.sql.Connection;
import java.util.*;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.BaseIdentifierConverter;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.jdbc.*;

/**
 * This class implements a Redshift-specific metadata reader.
 */
public class RedshiftMetadataReader extends AbstractRemoteMetadataReader {
    /**
     * Create a new instance of the {@link RedshiftMetadataReader}.
     *
     * @param connection JDBC connection to the remote data source
     * @param properties user-defined adapter properties
     */
    public RedshiftMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    protected ColumnMetadataReader createColumnMetadataReader() {
        return new RedshiftColumnMetadataReader(this.connection, this.properties, getIdentifierConverter());
    }

    @Override
    protected TableMetadataReader createTableMetadataReader() {
        return new RedshiftTableMetadataReader(this.connection, getColumnMetadataReader(), this.properties,
                getIdentifierConverter());
    }

    @Override
    public Set<String> getSupportedTableTypes() {
        return Collections
                .unmodifiableSet(new HashSet<>(Arrays.asList("TABLE", "VIEW", "SYSTEM TABLE", "EXTERNAL TABLE")));
    }

    @Override
    protected IdentifierConverter createIdentifierConverter() {
        return BaseIdentifierConverter.createDefault();
    }
}