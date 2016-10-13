package com.exasol.adapter.request;

import java.util.List;

import com.exasol.adapter.metadata.SchemaMetadataInfo;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.sql.SqlStatement;

public class PushdownRequest extends AdapterRequest {
    
    private SqlStatement select;
    private List<TableMetadata> involvedTablesMetadata;
    
    public PushdownRequest(SchemaMetadataInfo schemaMetadataInfo, SqlStatement select, List<TableMetadata> involvedTablesMetadata) {
        super(schemaMetadataInfo, AdapterRequestType.PUSHDOWN);
        this.select = select;
        this.involvedTablesMetadata = involvedTablesMetadata;
    }
    
    public SqlStatement getSelect() {
        return select;
    }

    public List<TableMetadata> getInvolvedTablesMetadata() {
        return involvedTablesMetadata;
    }
}
