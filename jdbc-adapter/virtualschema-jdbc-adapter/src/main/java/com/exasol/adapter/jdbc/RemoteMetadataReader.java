package com.exasol.adapter.jdbc;

import java.sql.DatabaseMetaData;

import com.exasol.adapter.metadata.SchemaMetadata;

/**
 * Common interface for all remote metadata readers independently of the SQL dialect.
 */
public interface RemoteMetadataReader {
    /**
     * Read schema metadata from the remote data source.
     *
     * @return remote schema metadata
     */
    public SchemaMetadata readRemoteSchemaMetadata();

    /**
     * Create the schema adapter notes from database metadata.
     *
     * @return schema adapter notes
     */
    public SchemaAdapterNotes createSchemaAdapterNotes(final DatabaseMetaData metadata);
}