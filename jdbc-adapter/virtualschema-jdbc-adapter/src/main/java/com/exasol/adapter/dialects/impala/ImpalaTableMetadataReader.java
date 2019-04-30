package com.exasol.adapter.dialects.impala;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.postgresql.PostgreSQLTableMetadataReader;
import com.exasol.adapter.jdbc.*;

/**
 * This class implements a reader for Impala database metadata
 */
public class ImpalaTableMetadataReader extends BaseTableMetadataReader {

    /**
     * Create a new {@link PostgreSQLTableMetadataReader} instance
     *
     * @param columnMetadataReader reader to be used to map the metadata of the tables columns
     * @param properties           user-defined adapter properties
     */
    public ImpalaTableMetadataReader(final ColumnMetadataReader columnMetadataReader,
            final AdapterProperties properties) {
        super(columnMetadataReader, properties);
    }

    @Override
    public IdentifierCaseHandling getUnquotedIdentifierHandling() {
        return IdentifierCaseHandling.INTERPRET_AS_LOWER;
    }

    @Override
    public IdentifierCaseHandling getQuotedIdentifierHandling() {
        return IdentifierCaseHandling.INTERPRET_AS_LOWER;
    }
}
