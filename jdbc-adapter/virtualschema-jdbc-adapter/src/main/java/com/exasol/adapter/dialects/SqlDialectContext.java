package com.exasol.adapter.dialects;

import com.exasol.adapter.jdbc.SchemaAdapterNotes;
import com.exasol.adapter.metadata.DataType;

/**
 * Context information required by {@link SqlDialect}
 */
public class SqlDialectContext {
    private final SchemaAdapterNotes schemaAdapterNotes;
    private final ImportType importType;
    private final PostgreSQLIdentifierMapping postgreSQLIdentifierMapping;
    private final DataType oracleNumberTargetType;

    public SqlDialectContext(final SchemaAdapterNotes schemaAdapterNotes) {
        this(schemaAdapterNotes, PostgreSQLIdentifierMapping.CONVERT_TO_UPPER, ImportType.JDBC);
    }

    public SqlDialectContext(final SchemaAdapterNotes schemaAdapterNotes, final PostgreSQLIdentifierMapping postgreSQLIdentifierMapping) {
        this(schemaAdapterNotes, postgreSQLIdentifierMapping, ImportType.JDBC);
    }

    public SqlDialectContext(final SchemaAdapterNotes schemaAdapterNotes, final PostgreSQLIdentifierMapping postgreSQLIdentifierMapping, final ImportType importType) {
        this(schemaAdapterNotes, postgreSQLIdentifierMapping, importType, DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, DataType.ExaCharset.UTF8));
    }

    public SqlDialectContext(final SchemaAdapterNotes schemaAdapterNotes, final PostgreSQLIdentifierMapping postgreSQLIdentifierMapping, final DataType oracleNumberTargetType) {
        this(schemaAdapterNotes, postgreSQLIdentifierMapping, ImportType.JDBC, oracleNumberTargetType);
    }

    public SqlDialectContext(final SchemaAdapterNotes schemaAdapterNotes, final PostgreSQLIdentifierMapping postgreSQLIdentifierMapping, final ImportType importType, final DataType oracleNumberTargetType) {
        this.schemaAdapterNotes = schemaAdapterNotes;
        this.postgreSQLIdentifierMapping = postgreSQLIdentifierMapping;
        this.importType = importType;
        this.oracleNumberTargetType = oracleNumberTargetType;
    }

    public SchemaAdapterNotes getSchemaAdapterNotes() {
        return this.schemaAdapterNotes;
    }

    public ImportType getImportType() { return this.importType; }

    public PostgreSQLIdentifierMapping getPostgreSQLIdentifierMapping() {
        return this.postgreSQLIdentifierMapping;
    }

    public DataType getOracleNumberTargetType() {
        return this.oracleNumberTargetType;
    }
}
