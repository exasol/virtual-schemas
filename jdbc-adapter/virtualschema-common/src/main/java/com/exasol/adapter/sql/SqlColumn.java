package com.exasol.adapter.sql;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.metadata.ColumnMetadata;

public class SqlColumn extends SqlNode {
    
    private int id;
    private ColumnMetadata metadata;
    private String tableName;

    public SqlColumn(int id, ColumnMetadata metadata) {
        this.id = id;
        this.metadata = metadata;
    }

    public SqlColumn(int id, ColumnMetadata metadata, String tableName) {
        this.id = id;
        this.metadata = metadata;
        this.tableName = tableName;
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

    public String getTableName() {
        return tableName;
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
    public <R> R accept(SqlNodeVisitor<R> visitor) throws AdapterException {
        return visitor.visit(this);
    }

}
