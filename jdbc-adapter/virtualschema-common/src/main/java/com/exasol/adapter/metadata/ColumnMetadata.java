package com.exasol.adapter.metadata;


/**
 * Represents the metadata of an EXASOL table column.
 */
public class ColumnMetadata {

    private String name;
    private String adapterNotes;
    private DataType type;
    private boolean isNullable; // has a third state "unknown" in jdbc, which has to be mapped by the adapter to yes or no
    private boolean isIdentity; // auto increment in the sense of jdbc. However, jdbc has a third state ("empty", i.e. could not determine) which is mapped to false here.
    private String defaultValue;    // special case: "NULL" means SQL's NULL.
    private String comment; // comes from "REMARKS" field in jdbc
    // Primary Keys?!?! => Index, Cardinality = num rows
    
    public ColumnMetadata(String name, String adapterNotes, DataType type, boolean nullable, boolean isIdentity, String defaultValue, String comment) {
        this.name = name;
        this.adapterNotes = adapterNotes;
        this.type = type;
        this.isNullable = nullable;
        this.isIdentity = isIdentity;
        this.defaultValue = defaultValue;
        this.comment = comment;
    }
    
    public String getName() {
        return name;
    }
    
    public String getAdapterNotes() {
        return adapterNotes;
    }
    
    public DataType getType() {
        return type;
    }
    
    public boolean isNullable() {
        return isNullable;
    }
    
    public boolean isIdentity() {
        return isIdentity;
    }
    
    public boolean hasDefault() {
        return !defaultValue.isEmpty();
    }
    
    public String getDefaultValue() {
        return defaultValue;
    }
    
    public boolean hasComment() {
        return !comment.isEmpty();
    }
    
    public String getComment() {
        return comment;
    }
    
}
