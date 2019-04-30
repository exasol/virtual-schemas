package com.exasol.adapter.jdbc;

public interface MetadataReader {
    /**
     * Get the catalog name that is applied as filter criteria when looking up remote metadata
     *
     * @return catalog name or <code>null</code> if metadata lookups are not limited by catalog
     */
    String getCatalogNameFilter();

    /**
     * Get the schema name that is applied as filter criteria when looking up remote metadata
     *
     * @return schema name or <code>null</code> if metadata lookups are not limited by schema
     */
    String getSchemaNameFilter();

    /**
     * @return How to handle case sensitivity of unquoted identifiers
     */
    public IdentifierCaseHandling getUnquotedIdentifierHandling();

    /**
     * @return How to handle case sensitivity of quoted identifiers
     */
    public IdentifierCaseHandling getQuotedIdentifierHandling();
}