package com.exasol.adapter.sql;

import com.exasol.adapter.metadata.ColumnMetadata;

public class SqlColumn extends SqlNode {
    
    private int id;
    private ColumnMetadata metadata;

    public SqlColumn(int id, ColumnMetadata metadata) {
        this.id = id;
        this.metadata = metadata;
    }
    
    public int getId() {
        return id;
    }
    
    public ColumnMetadata getMetadata() {
        return metadata;
    }
    
    public String getName() {
        return metadata.getName();
    }
    
    @Override
    public String toSimpleSql() {
        return "\"" + metadata.getName().replace("\"", "\"\"") + "\"";
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.COLUMN;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) {
        return visitor.visit(this);
    }

}
