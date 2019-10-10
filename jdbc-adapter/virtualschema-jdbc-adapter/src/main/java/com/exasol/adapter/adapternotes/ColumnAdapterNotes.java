package com.exasol.adapter.adapternotes;

import javax.json.*;

import com.exasol.adapter.AdapterException;
import com.exasol.utils.JsonHelper;

/**
 * Serializes and deserializes the column adapter notes specific to the JDBC Adapter.
 */
public class ColumnAdapterNotes {
    private static final String TYPE_NAME = "typeName";
    private static final String JDBC_DATA_TYPE = "jdbcDataType";
    private final int jdbcDataType;
    private final String typeName;

    /**
     * Create a new instance of the {@link ColumnAdapterNotes}.
     *
     * @param jdbcDataType JDBC data type in int format
     * @param typeName     name of the data type
     */
    public ColumnAdapterNotes(final int jdbcDataType, final String typeName) {
        this.jdbcDataType = jdbcDataType;
        this.typeName = typeName;
    }

    /**
     * Get JDBC data type.
     * 
     * @return JDBC data type as an int
     */
    public int getJdbcDataType() {
        return this.jdbcDataType;
    }

    /**
     * Get JDBC type name.
     * 
     * @return JDBC type name as a string
     */
    public String getTypeName() {
        return this.typeName;
    }

    /**
     * Serialized column adapter notes.
     * 
     * @param notes column adapter notes
     * @return serialized column adapter notes
     */
    public static String serialize(final ColumnAdapterNotes notes) {
        final JsonBuilderFactory factory = JsonHelper.getBuilderFactory();
        final JsonObjectBuilder builder = factory.createObjectBuilder().add(JDBC_DATA_TYPE, notes.getJdbcDataType())
                .add(TYPE_NAME, notes.getTypeName());
        return builder.build().toString();
    }

    /**
     * Deserialize column adapter notes.
     *
     * @param columnAdapterNotes column adapter notes
     * @param columnName column name
     * @return deserialized column adapter notes
     * @throws AdapterException if column adapter notes are empty or null
     */
    public static ColumnAdapterNotes deserialize(final String columnAdapterNotes, final String columnName)
            throws AdapterException {
        if ((columnAdapterNotes == null) || columnAdapterNotes.isEmpty()) {
            throw new AdapterException("The adapternotes field of column " + columnName
                    + " are empty or null. Please refresh the virtual schema. ");
        }
        final JsonObject root;
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