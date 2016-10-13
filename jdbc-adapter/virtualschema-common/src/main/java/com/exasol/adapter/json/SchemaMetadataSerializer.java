package com.exasol.adapter.json;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObjectBuilder;

import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.utils.JsonHelper;

public class SchemaMetadataSerializer {
    
    public static JsonObjectBuilder serialize(SchemaMetadata schema) {
        JsonBuilderFactory factory = JsonHelper.getBuilderFactory();
        JsonObjectBuilder root = factory.createObjectBuilder();
        JsonArrayBuilder tablesBuilder = factory.createArrayBuilder();
        for (TableMetadata table : schema.getTables()) {
            tablesBuilder.add(serializeTableMetadata(table, factory.createObjectBuilder()));
        }
        root.add("tables", tablesBuilder);
        root.add("adapterNotes", schema.getAdapterNotes());
        return root;
    }
    
    private static JsonObjectBuilder serializeTableMetadata(TableMetadata table, JsonObjectBuilder tableBuilder) {
        tableBuilder.add("type", "table");
        tableBuilder.add("name", table.getName());
        JsonArrayBuilder columnsBuilder = Json.createArrayBuilder();
        for (ColumnMetadata column : table.getColumns()) {
            columnsBuilder.add(serializeColumnMetadata(column, Json.createObjectBuilder()));
        }
        tableBuilder.add("adapterNotes", table.getAdapterNotes());
        if (table.hasComment()) {
            tableBuilder.add("comment", table.getComment());
        }
        tableBuilder.add("columns", columnsBuilder);
        return tableBuilder;
    }
    
    private static JsonObjectBuilder serializeColumnMetadata(ColumnMetadata column, JsonObjectBuilder columnBuilder) {
        columnBuilder.add("name", column.getName());
        columnBuilder.add("adapterNotes", column.getAdapterNotes());
        columnBuilder.add("dataType", SqlDataTypeJsonSerializer.serialize(column.getType()));
        if (!column.isNullable()) {
            columnBuilder.add("isNullable", false);
        }
        if (column.isIdentity()) {
            columnBuilder.add("isIdentity", true);
        }
        if (column.hasDefault()) {
            columnBuilder.add("default", column.getDefaultValue());
        }
        if (column.hasComment()) {
            columnBuilder.add("comment", column.getComment());
        }
        return columnBuilder;
    }

}
