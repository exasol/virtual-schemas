package com.exasol.adapter.request;

import java.util.List;

import com.exasol.adapter.metadata.SchemaMetadataInfo;

public class RefreshRequest extends AdapterRequest {
    
    private boolean isRefreshForTables;
    private List<String> tables;
    
    public RefreshRequest(SchemaMetadataInfo schemaMetadataInfo) {
        super(schemaMetadataInfo, AdapterRequestType.REFRESH);
        isRefreshForTables = false;
    }
    
    public RefreshRequest(SchemaMetadataInfo schemaMetadataInfo, List<String> tables) {
        super(schemaMetadataInfo, AdapterRequestType.REFRESH);
        assert(tables != null && !tables.isEmpty());
        isRefreshForTables = true;
        this.tables = tables;
    }
    
    public List<String> getTables() {
        return tables;
    }
    
    public boolean isRefreshForTables() {
        return isRefreshForTables;
    }
    
}
