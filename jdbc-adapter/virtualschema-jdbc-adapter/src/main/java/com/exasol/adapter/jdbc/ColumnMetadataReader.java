package com.exasol.adapter.jdbc;

import java.util.List;

import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;

/**
 * Common interface for all remote column metadata readers.
 */
public interface ColumnMetadataReader {
    /**
     * Map a metadata for a list of columns to Exasol metadata.
     *
     * @param tableName the table for which the columns are mapped
     * @return list of Exasol column metadata objects
     */
    public List<ColumnMetadata> mapColumns(String tableName);

    /**
     * Map type information from JDBC to the Exasol type information.
     * <p>
     * Override this method in a dedicated mapper if you need dialect specific behavior.
     * </p>
     *
     * @param jdbcTypeDescription parameter object describing the type from the JDBC perspective
     * @return Exasol data type information
     */
    public DataType mapJdbcType(JdbcTypeDescription jdbcTypeDescription);
}