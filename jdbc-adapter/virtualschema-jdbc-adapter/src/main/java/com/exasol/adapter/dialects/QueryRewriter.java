package com.exasol.adapter.dialects;

import java.sql.SQLException;

import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.sql.SqlStatement;

/**
 * This class is the common interface for all builders that re-write the push-down query.
 * <p>
 * The Virtual Schema frontend sends a push-down query to the Virtual Schema backend and expects an SQL query back. In
 * most cases this will be an <code>IMPORT</code> statement because it is easy to get this fast. But that is not the
 * only option.
 * </p>
 * <p>
 * <code>SELECT ... FROM (VALUES ...)</code> is another candidate that works in case of data sources that the Exasol
 * core database can't import from directly (e.g. NoSQL databases).
 * </p>
 * <p>
 * Last but not least you can combine your Virtual Schema Adapter with another UDF. In that case have the rewriter
 * create a SQL statement that triggers an import through that UDF. This solution can be beneficial from a performance
 * perspective since you can parallelize it.
 * </p>
 */
public interface QueryRewriter {
    /**
     * Set the original push-down statement.
     *
     * @param statement   SQL statement that represents the original push-down query
     * @param properties  user-defined adapter properties
     * @param exaMetadata Exasol metadata
     * @return rewritten query
     * @throws AdapterException if rewriting fails
     * @throws SQLException     if any SQL commands executed on the remote data source failed during rewriting
     */
    public String rewrite(final SqlStatement statement, final ExaMetadata exaMetadata,
            final AdapterProperties properties) throws AdapterException, SQLException;
}