package com.exasol.adapter.jdbc;

import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.utils.JsonHelper;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

/**
 * Serializes and deserializes the column adapter notes specific to the JDBC Adapter
 */
public class ColumnAdapterNotes {

    private int jdbcDataType;
    private String typeName;

    public ColumnAdapterNotes(int jdbcDataType, String typeName) {
        this.jdbcDataType = jdbcDataType;
        this.typeName = typeName;
    }

    public int getJdbcDataType() {
        return jdbcDataType;
    }

    public String getTypeName() {
        return typeName;
    }

    public static String serialize(ColumnAdapterNotes notes) {
        JsonBuilderFactory factory = JsonHelper.getBuilderFactory();
        JsonObjectBuilder builder = factory.createObjectBuilder()
                .add("jdbcDataType", notes.getJdbcDataType())
                .add("typeName", notes.getTypeName());
        return builder.build().toString();
    }

    public static ColumnAdapterNotes deserialize(String columnAdapterNotes, String columnName) {
        if (columnAdapterNotes == null || columnAdapterNotes.isEmpty()) {
            throw new RuntimeException(getException(columnName));
        }
        JsonObject root;
        try {
            root = JsonHelper.getJsonObject(columnAdapterNotes);
        } catch (Exception ex) {
            throw new RuntimeException(getException(columnName));
        }
        checkKey(root, "jdbcDataType", columnName);
        checkKey(root, "typeName", columnName);
        return new ColumnAdapterNotes(
                root.getInt("jdbcDataType"),
                root.getString("typeName"));
    }

    private static void checkKey(JsonObject root, String key, String columnName) {
        if (!root.containsKey(key)) {
            throw new RuntimeException(getException(columnName));
        }
    }

    private static String getException(String columnName) {
        return "The adapternotes field of column " + columnName + " could not be parsed. Please refresh the virtual schema.";
    }
}
