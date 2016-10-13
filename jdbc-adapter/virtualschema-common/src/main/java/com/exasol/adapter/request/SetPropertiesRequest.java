package com.exasol.adapter.request;

import java.util.Map;

import com.exasol.adapter.metadata.SchemaMetadataInfo;

public class SetPropertiesRequest extends AdapterRequest {
    
    private Map<String, String> properties;
    
    public SetPropertiesRequest(SchemaMetadataInfo schemaMetadataInfo, Map<String, String> properties) {
        super(schemaMetadataInfo, AdapterRequestType.SET_PROPERTIES);
        this.properties = properties;
    }
    
    public Map<String, String> getProperties() {
        return properties;
    }
}
