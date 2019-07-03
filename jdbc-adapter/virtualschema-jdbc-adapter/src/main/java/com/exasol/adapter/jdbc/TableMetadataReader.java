package com.exasol.adapter.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import com.exasol.adapter.metadata.TableMetadata;

/**
 * Common interface for all table metadata readers.
 */
public interface TableMetadataReader {
    /**
     * Map a list of tables found in a JDBC result set to a list of {@link TableMetadata}.
     *
     * @param remoteTables   result set containing the tables to be mapped
     * @param selectedTables optional list of tables name that is used to narrow down the mapping
     * @return list of {@link TableMetadata}
     * @throws SQLException if either mapping the table or its columns produces an SQL error
     */
    public List<TableMetadata> mapTables(ResultSet remoteTables, Optional<List<String>> selectedTables)
            throws SQLException;

    /**
     * Returns true if the metadata reader includes the table with the given name when the remote metadata gets mapped
     * table name.
     *
     * @param tableName name of the table
     * @return <code>true</code> if the reader includes the the mapping, <code>false</code> if it is ignored
     */
    public boolean isTableIncludedByMapping(final String tableName);
}