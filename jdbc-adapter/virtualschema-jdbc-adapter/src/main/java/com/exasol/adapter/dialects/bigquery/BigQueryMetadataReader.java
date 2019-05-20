package com.exasol.adapter.dialects.bigquery;

import java.sql.*;

import com.exasol.adapter.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.*;

/**
 * Metadata reader that reads BigQuery-specific database metadata
 */
public class BigQueryMetadataReader extends BaseRemoteMetadataReader {
    /**
     * Create a new instance of an {@link BigQueryMetadataReader}
     *
     * @param connection JDBC connection to the remote data source
     * @param properties user-defined adapter properties
     */
    public BigQueryMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }
}
