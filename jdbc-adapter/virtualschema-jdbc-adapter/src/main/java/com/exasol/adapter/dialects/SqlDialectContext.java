package com.exasol.adapter.dialects;

import com.exasol.adapter.jdbc.SchemaAdapterNotes;

/**
 * Context information required by {@link SqlDialect}
 */
public class SqlDialectContext {
    private SchemaAdapterNotes schemaAdapterNotes;
    private PostgreSQLIdentifierMapping postgreSQLIdentifierMapping;

    public SqlDialectContext(SchemaAdapterNotes schemaAdapterNotes) {
        this.schemaAdapterNotes = schemaAdapterNotes;
        this.postgreSQLIdentifierMapping = PostgreSQLIdentifierMapping.CONVERT_TO_UPPER;
    }

    public SqlDialectContext(SchemaAdapterNotes schemaAdapterNotes, PostgreSQLIdentifierMapping postgreSQLIdentifierMapping) {
        this.schemaAdapterNotes = schemaAdapterNotes;
        this.postgreSQLIdentifierMapping = postgreSQLIdentifierMapping;
    }

    public SchemaAdapterNotes getSchemaAdapterNotes() {
        return schemaAdapterNotes;
    }

    public PostgreSQLIdentifierMapping getPostgreSQLIdentifierMapping() {
        return postgreSQLIdentifierMapping;
    }
}
