package com.exasol.adapter.sql;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.metadata.TableMetadata;

public class SqlTable extends SqlNode {
    
    private String name;
    private String alias;   // what is the exact semantic of this? Currently simply to generate a query with the expected alias.
    private TableMetadata metadata;

    public SqlTable(String name, TableMetadata metadata) {
        this.name = name;
        this.alias = name;
        this.metadata = metadata;
    }

    public SqlTable(String name, String alias, TableMetadata metadata) {
        this.name = name;
        this.alias = alias;
        this.metadata = metadata;
    }
    
    public boolean hasAlias() {
        return !name.equals(alias);
    }
    
    public String getName() {
        return name;
    }
    
    public String getAlias() {
        return alias;
    }
    
    public TableMetadata getMetadata() {
        return metadata;
    }
    
    @Override
    public String toSimpleSql() {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.TABLE;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) throws AdapterException {
        return visitor.visit(this);
    }

}
