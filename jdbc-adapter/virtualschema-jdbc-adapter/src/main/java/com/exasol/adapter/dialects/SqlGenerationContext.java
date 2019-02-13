package com.exasol.adapter.dialects;

/**
 * Context information needed during SQL generation. These information are not dialect specific.
 *
 * Contains information that are globally available during sql generation, but not part of the SqlNode graph.
 */
public class SqlGenerationContext {

    private String catalogName;
    private String schemaName;
    private boolean isLocal;
    private boolean hasMoreThanOneTable;
    
    public SqlGenerationContext(String catalogName, String schemaName, boolean isLocal, boolean hasMoreThanOneTable) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.isLocal = isLocal;
        this.hasMoreThanOneTable = hasMoreThanOneTable;
    }
    
    public String getCatalogName() {
        return catalogName;
    }
    
    public String getSchemaName() {
        return schemaName;
    }
    
    public boolean isLocal() {
        return isLocal;
    }

    public boolean hasMoreThanOneTable() {
        return hasMoreThanOneTable;
    }
    
}
