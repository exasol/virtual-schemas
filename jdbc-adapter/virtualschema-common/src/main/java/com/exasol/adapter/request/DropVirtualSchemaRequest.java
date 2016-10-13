package com.exasol.adapter.request;

import com.exasol.adapter.metadata.SchemaMetadataInfo;

public class DropVirtualSchemaRequest extends AdapterRequest {
    
    public DropVirtualSchemaRequest(SchemaMetadataInfo schemaMetadataInfo) {
        super(schemaMetadataInfo, AdapterRequestType.DROP_VIRTUAL_SCHEMA);
    }
}
