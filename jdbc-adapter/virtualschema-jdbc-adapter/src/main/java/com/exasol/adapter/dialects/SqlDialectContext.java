package com.exasol.adapter.dialects;

import com.exasol.adapter.jdbc.SchemaAdapterNotes;

/**
 * Context information required by {@link SqlDialect}
 */
public class SqlDialectContext {

    private SchemaAdapterNotes schemaAdapterNotes;

    public SqlDialectContext(SchemaAdapterNotes schemaAdapterNotes) {
        this.schemaAdapterNotes = schemaAdapterNotes;
    }

    public SchemaAdapterNotes getSchemaAdapterNotes() {
        return schemaAdapterNotes;
    }
}
