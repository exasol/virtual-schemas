package com.exasol.adapter.metadata;

import com.google.common.base.MoreObjects;

import java.util.List;

/**
 * Represents the metadata of an EXASOL table.
 */
public class TableMetadata {

    private String name;
    private String adapterNotes;
    private List<ColumnMetadata> columns;
    private String comment;
    
    public TableMetadata(String name, String adapterNotes, List<ColumnMetadata> columns, String comment) throws MetadataException {
        this.name = name;
        this.adapterNotes = adapterNotes;
        this.columns = columns;
        this.comment = comment;
        if (this.columns.isEmpty()) {
            throw new MetadataException("A table without columns was encountered: " + this.name + ". This is not supported." +
                    " Please check if this table has columns. If the table does have columns, the dialect probably does not properly handle the data types of the columns.");
        }
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .add("adapterNotes", adapterNotes)
                .add("columns", columns)
                .add("comment", comment)
                .toString();
    }
    
    public String getName() {
        return name;
    }
    
    public String getAdapterNotes() {
        return adapterNotes;
    }
    
    public List<ColumnMetadata> getColumns() {
        return columns;
    }
    
    public boolean hasComment() {
        return !comment.isEmpty();
    }
    
    public String getComment() {
        return comment;
    }
    
}
