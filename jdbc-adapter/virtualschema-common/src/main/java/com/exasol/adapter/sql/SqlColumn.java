package com.exasol.adapter.sql;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.metadata.ColumnMetadata;

public class SqlColumn extends SqlNode {
    
    private int id;
    private ColumnMetadata metadata;
    private String tableName;
    private String tableAlias;

    public SqlColumn(int id, ColumnMetadata metadata) {
        this.id = id;
        this.metadata = metadata;
        this.tableName = null;
        this.tableAlias = null;
    }

    public SqlColumn(int id, ColumnMetadata metadata, String tableName) {
        this.id = id;
        this.metadata = metadata;
        this.tableName = tableName;
        this.tableAlias = null;
    }

    public SqlColumn(int id, ColumnMetadata metadata, String tableName, String tableAlias) {
        this.id = id;
        this.metadata = metadata;
        this.tableName = tableName;
        this.tableAlias = tableAlias;
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

    public boolean hasTableAlias() { return this.tableAlias != null; }

    public String getTableAlias() {
        return tableAlias;
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
