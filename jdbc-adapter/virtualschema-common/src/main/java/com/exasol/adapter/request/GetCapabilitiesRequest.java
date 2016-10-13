package com.exasol.adapter.request;

import com.exasol.adapter.metadata.SchemaMetadataInfo;

public class GetCapabilitiesRequest extends AdapterRequest {
    
    public GetCapabilitiesRequest(SchemaMetadataInfo schemaMetadataInfo) {
        super(schemaMetadataInfo, AdapterRequestType.GET_CAPABILITIES);
    }
}
