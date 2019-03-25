package com.exasol.adapter.dialects;

import com.exasol.adapter.jdbc.SchemaAdapterNotes;
import com.exasol.adapter.metadata.DataType;

import javax.xml.crypto.Data;
import java.util.Collections;
import java.util.Map;

/**
 * Context information required by {@link SqlDialect}
 */
public class SqlDialectContext {
    private SchemaAdapterNotes schemaAdapterNotes;
    private ImportType importType;
    private PostgreSQLIdentifierMapping postgreSQLIdentifierMapping;
    private DataType oracleCastNumberToDecimal;

    public SqlDialectContext(SchemaAdapterNotes schemaAdapterNotes) {
        this(schemaAdapterNotes, PostgreSQLIdentifierMapping.CONVERT_TO_UPPER, ImportType.JDBC);
    }

    public SqlDialectContext(SchemaAdapterNotes schemaAdapterNotes, PostgreSQLIdentifierMapping postgreSQLIdentifierMapping) {
        this(schemaAdapterNotes, postgreSQLIdentifierMapping, ImportType.JDBC);
    }

    public SqlDialectContext(SchemaAdapterNotes schemaAdapterNotes, PostgreSQLIdentifierMapping postgreSQLIdentifierMapping, ImportType importType) {
        this(schemaAdapterNotes, postgreSQLIdentifierMapping, importType, DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, DataType.ExaCharset.UTF8));
    }

    public SqlDialectContext(SchemaAdapterNotes schemaAdapterNotes, PostgreSQLIdentifierMapping postgreSQLIdentifierMapping, DataType oracleCastNumberToDecimal) {
        this(schemaAdapterNotes, postgreSQLIdentifierMapping, ImportType.JDBC, oracleCastNumberToDecimal);
    }

    public SqlDialectContext(SchemaAdapterNotes schemaAdapterNotes, PostgreSQLIdentifierMapping postgreSQLIdentifierMapping, ImportType importType, DataType oracleCastNumberToDecimal) {
        this.schemaAdapterNotes = schemaAdapterNotes;
        this.postgreSQLIdentifierMapping = postgreSQLIdentifierMapping;
        this.importType = importType;
        this.oracleCastNumberToDecimal = oracleCastNumberToDecimal;
    }

    public SchemaAdapterNotes getSchemaAdapterNotes() {
        return schemaAdapterNotes;
    }

    public ImportType getImportType() { return importType; }

    public PostgreSQLIdentifierMapping getPostgreSQLIdentifierMapping() {
        return postgreSQLIdentifierMapping;
    }

    public DataType getOracleCastNumberToDecimal() {
        return oracleCastNumberToDecimal;
    }
}
