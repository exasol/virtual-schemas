package com.exasol.adapter.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.exasol.adapter.dialects.SqlDialect.IdentifierCaseHandling;
import com.exasol.adapter.metadata.TableMetadata;

/**
 * Common interface for all table metadata readers
 */
public interface TableMetadataReader {
    /**
     * Map a list of tables found in a JDBC result set to a list of {@link TableMetadata}
     *
     * @param remoteTables result set containing the tables to be mapped
     * @return list of {@link TableMetadata}
     * @throws SQLException if either mapping the table or its columns produces an SQL error
     */
    public List<TableMetadata> mapTables(ResultSet remoteTables) throws SQLException;

    /**
     * Adjust the case of the identifier as required and configured
     *
     * @param identifier original identifier
     * @return case-adjusted identifier
     */
    public String adjustIdentifierCase(String identifier);

    /**
     * Define how the metadata reader handles unquoted identifiers
     *
     * @return case handling for unquoted identifiers
     */
    public IdentifierCaseHandling getUnquotedIdentifierCaseHandling();

    /**
     * Define how the metadata reader handles quoted identifiers
     *
     * @return case handling for quoted identifiers
     */
    public IdentifierCaseHandling getQuotedIdentifierCaseHandling();

    /**
     * Returns true if the metadata reader includes the table with the given name when the remote metadata gets mapped
     *
     * @param tableName name of the table
     * @return <code>true</code> if the reader includes the the mapping, <code>false</code> if it is ignored
     */
    public boolean isTableIncludedByMapping(final String tableName);
}