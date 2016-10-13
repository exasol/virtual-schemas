package com.exasol.adapter.request;

import com.exasol.adapter.metadata.SchemaMetadataInfo;

public class CreateVirtualSchemaRequest extends AdapterRequest {
    
    public CreateVirtualSchemaRequest(SchemaMetadataInfo schemaMetadataInfo) {
        super(schemaMetadataInfo, AdapterRequestType.CREATE_VIRTUAL_SCHEMA);
    }
}
