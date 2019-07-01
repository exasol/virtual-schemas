package com.exasol.adapter.dialects;

/**
 * Context information needed during SQL generation. These information are not dialect specific.
 *
 * Contains information that are globally available during sql generation, but not part of the SqlNode graph.
 */
public class SqlGenerationContext {
    private final String catalogName;
    private final String schemaName;
    private final boolean isLocal;

    /**
     * Creates a new instance of the {@link SqlGenerationContext}.
     *
     * @param catalogName catalog name as a string
     * @param schemaName schema name as a string
     * @param isLocal true if import is local
     */
    public SqlGenerationContext(final String catalogName, final String schemaName, final boolean isLocal) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.isLocal = isLocal;
    }

    /**
     * Get a catalog's name.
     *
     * @return catalog name
     */
    public String getCatalogName() {
        return catalogName;
    }

    /**
     * Get a schema's name.
     *
     * @return schema name
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * Get true if import is local.
     *
     * @return true if import is local
     */
    public boolean isLocal() {
        return isLocal;
    }
}
