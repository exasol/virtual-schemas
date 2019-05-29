package com.exasol.adapter.jdbc;

import javax.json.*;

import com.exasol.adapter.AdapterException;
import com.exasol.utils.JsonHelper;

/**
 * Serializes and deserializes the column adapter notes specific to the JDBC Adapter
 */
public class ColumnAdapterNotes {
    private static final String TYPE_NAME = "typeName";
    private static final String JDBC_DATA_TYPE = "jdbcDataType";
    private final int jdbcDataType;
    private final String typeName;

    public ColumnAdapterNotes(final int jdbcDataType, final String typeName) {
        this.jdbcDataType = jdbcDataType;
        this.typeName = typeName;
    }

    public int getJdbcDataType() {
        return this.jdbcDataType;
    }

    public String getTypeName() {
        return this.typeName;
    }

    public static String serialize(final ColumnAdapterNotes notes) {
        final JsonBuilderFactory factory = JsonHelper.getBuilderFactory();
        final JsonObjectBuilder builder = factory.createObjectBuilder().add(JDBC_DATA_TYPE, notes.getJdbcDataType())
                .add(TYPE_NAME, notes.getTypeName());
        return builder.build().toString();
    }

    public static ColumnAdapterNotes deserialize(final String columnAdapterNotes, final String columnName)
            throws AdapterException {
        if ((columnAdapterNotes == null) || columnAdapterNotes.isEmpty()) {
            throw new AdapterException("The adapternotes field of column " + columnName
                    + " are empty or null.Please refresh the virtual schema. ");
        }
        JsonObject root;
        try {
            root = JsonHelper.getJsonObject(columnAdapterNotes);
        } catch (final Exception ex) {
            throw new AdapterException("Can not get the json object for column notes of column " + columnName
                    + ". Please refresh the virtual schema");
        }
        checkKey(root, JDBC_DATA_TYPE, columnName);
        checkKey(root, TYPE_NAME, columnName);
        return new ColumnAdapterNotes(root.getInt(JDBC_DATA_TYPE), root.getString(TYPE_NAME));
    }

    private static void checkKey(final JsonObject root, final String key, final String columnName)
            throws AdapterException {
        if (!root.containsKey(key)) {
            throw new AdapterException("Adapter notes of column " + columnName + " don't have the key " + key
                    + ". Please refresh the virtual schema");
        }
    }
}