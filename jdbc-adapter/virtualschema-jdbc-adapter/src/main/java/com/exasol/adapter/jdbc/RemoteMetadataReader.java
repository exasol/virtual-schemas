package com.exasol.adapter.jdbc;

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
}