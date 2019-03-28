package com.exasol.adapter.dialects;

import com.exasol.adapter.jdbc.SchemaAdapterNotes;
import com.exasol.adapter.metadata.DataType;

/**
 * Context information required by {@link SqlDialect}
 */
public class SqlDialectContext {
    private SchemaAdapterNotes schemaAdapterNotes;
    private ImportType importType;
    private PostgreSQLIdentifierMapping postgreSQLIdentifierMapping;
    private DataType oracleCastNumberToType;

    public SqlDialectContext(SchemaAdapterNotes schemaAdapterNotes) {
        this(schemaAdapterNotes, PostgreSQLIdentifierMapping.CONVERT_TO_UPPER, ImportType.JDBC);
    }

    public SqlDialectContext(SchemaAdapterNotes schemaAdapterNotes, PostgreSQLIdentifierMapping postgreSQLIdentifierMapping) {
        this(schemaAdapterNotes, postgreSQLIdentifierMapping, ImportType.JDBC);
    }

    public SqlDialectContext(SchemaAdapterNotes schemaAdapterNotes, PostgreSQLIdentifierMapping postgreSQLIdentifierMapping, ImportType importType) {
        this(schemaAdapterNotes, postgreSQLIdentifierMapping, importType, DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, DataType.ExaCharset.UTF8));
    }

    public SqlDialectContext(SchemaAdapterNotes schemaAdapterNotes, PostgreSQLIdentifierMapping postgreSQLIdentifierMapping, DataType oracleCastNumberToType) {
        this(schemaAdapterNotes, postgreSQLIdentifierMapping, ImportType.JDBC, oracleCastNumberToType);
    }

    public SqlDialectContext(SchemaAdapterNotes schemaAdapterNotes, PostgreSQLIdentifierMapping postgreSQLIdentifierMapping, ImportType importType, DataType oracleCastNumberToType) {
        this.schemaAdapterNotes = schemaAdapterNotes;
        this.postgreSQLIdentifierMapping = postgreSQLIdentifierMapping;
        this.importType = importType;
        this.oracleCastNumberToType = oracleCastNumberToType;
    }

    public SchemaAdapterNotes getSchemaAdapterNotes() {
        return schemaAdapterNotes;
    }

    public ImportType getImportType() { return importType; }

    public PostgreSQLIdentifierMapping getPostgreSQLIdentifierMapping() {
        return postgreSQLIdentifierMapping;
    }

    public DataType getOracleCastNumberToType() {
        return oracleCastNumberToType;
    }
}
