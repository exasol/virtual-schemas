package com.exasol.adapter.dialects.sybase;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.BaseIdentifierConverter;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.dialects.sqlserver.SqlServerColumnMetadataReader;
import com.exasol.adapter.jdbc.*;

/**
 * This class implements a Sybase-specific remote metadata reader
 */
public class SybaseMetadataReader extends AbstractRemoteMetadataReader {
    /**
     * Create a new instance of a {@link SybaseMetadataReader}
     *
     * @param connection JDBC connection to the remote data source
     * @param properties user-defined adapter properties
     */
    public SybaseMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    /**
     * Create a column metadata reader
     * <p>
     * Due to the close relationship between Sybase and SQLSever, we reuse the SQLServer column metadata reader
     * here.
     */
    @Override
    protected ColumnMetadataReader createColumnMetadataReader() {
        return new SqlServerColumnMetadataReader(this.connection, this.properties, getIdentifierConverter());
    }

    @Override
    protected TableMetadataReader createTableMetadataReader() {
        return new BaseTableMetadataReader(this.connection, this.columnMetadataReader, this.properties,
                this.identifierConverter);
    }

    @Override
    protected IdentifierConverter createIdentifierConverter() {
        return BaseIdentifierConverter.createDefault();
    }
}