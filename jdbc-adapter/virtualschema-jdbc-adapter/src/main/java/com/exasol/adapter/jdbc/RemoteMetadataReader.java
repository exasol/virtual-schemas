package com.exasol.adapter.jdbc;

import java.util.List;
import java.util.Set;

import com.exasol.adapter.adapternotes.SchemaAdapterNotes;
import com.exasol.adapter.metadata.SchemaMetadata;

/**
 * Common interface for all remote metadata readers independently of the SQL dialect.
 */
public interface RemoteMetadataReader extends MetadataReader {
    /**
     * Read schema metadata from the remote data source.
     *
     * @return remote schema metadata
     */
    public SchemaMetadata readRemoteSchemaMetadata();

    /**
     * Read schema metadata for a selection of tables from the remote data source.
     *
     * @param tables selected tables
     * @return remote schema metadata
     */
    public SchemaMetadata readRemoteSchemaMetadata(List<String> tables);

    /**
     * Create the schema adapter notes from database metadata.
     *
     * @return schema adapter notes
     */
    public SchemaAdapterNotes getSchemaAdapterNotes();

    /**
     * Get the remote column metadata reader.
     *
     * @return column metadata reader
     */
    public ColumnMetadataReader getColumnMetadataReader();

    /**
     * Get the table metadata reader.
     *
     * @return table metadata reader
     */
    public TableMetadataReader getTableMetadataReader();

    /**
     * Get the table types the remote metadata reader supports.
     *
     * @return set of table type names
     */
    public Set<String> getSupportedTableTypes();
}