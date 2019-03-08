package com.exasol.adapter.dialects;

import com.exasol.adapter.jdbc.SchemaAdapterNotes;

/**
 * Context information required by {@link SqlDialect}
 */
public class SqlDialectContext {
    private SchemaAdapterNotes schemaAdapterNotes;
    private ImportType importType;
    private PostgreSQLIdentifierMapping postgreSQLIdentifierMapping;

    public SqlDialectContext(SchemaAdapterNotes schemaAdapterNotes) {
        this(schemaAdapterNotes, PostgreSQLIdentifierMapping.CONVERT_TO_UPPER, ImportType.JDBC);
    }

    public SqlDialectContext(SchemaAdapterNotes schemaAdapterNotes, PostgreSQLIdentifierMapping postgreSQLIdentifierMapping) {
        this(schemaAdapterNotes, postgreSQLIdentifierMapping, ImportType.JDBC);
    }

    public SqlDialectContext(SchemaAdapterNotes schemaAdapterNotes, PostgreSQLIdentifierMapping postgreSQLIdentifierMapping, ImportType importType) {
        this.schemaAdapterNotes = schemaAdapterNotes;
        this.postgreSQLIdentifierMapping = postgreSQLIdentifierMapping;
        this.importType = importType;
    }

    public SchemaAdapterNotes getSchemaAdapterNotes() {
        return schemaAdapterNotes;
    }

    public ImportType getImportType() { return importType; }

    public PostgreSQLIdentifierMapping getPostgreSQLIdentifierMapping() {
        return postgreSQLIdentifierMapping;
    }
}
