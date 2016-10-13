package com.exasol.adapter.metadata;

import java.util.Map;

import com.google.common.base.MoreObjects;

/**
 * Represents the metadata of an EXASOL Virtual Schema which are sent with each request.
 * The metadata are just "for information" for the adapter. These metadata don't contain the table metadata.
 */
public class SchemaMetadataInfo {
    
    private String schemaName;
    private String adapterNotes;
    private Map<String, String> properties;
    
    public SchemaMetadataInfo(String schemaName, String adapterNotes, Map<String, String> properties) {
        this.schemaName = schemaName;
        this.adapterNotes = adapterNotes;
        this.properties = properties;
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("schemaName", schemaName)
            .add("adapterNotes", adapterNotes)
            .add("properties", properties)
            .toString();
    }
    
    public String getSchemaName() {
        return schemaName;
    }
    
    public String getAdapterNotes() {
        return adapterNotes;
    }

    /**
     * \note Keys are case-insensitive and stored upper case
     */
    public Map<String, String> getProperties() {
        return properties;
    }

}
