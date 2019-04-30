package com.exasol.adapter.dialects.redshift;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.*;

/**
 * This class implements a reader for Redshift database metadata
 */
public class RedshiftTableMetadataReader extends BaseTableMetadataReader {

    /**
     * Create a new {@link RedshiftTableMetadataReader} instance
     *
     * @param columnMetadataReader reader to be used to map the metadata of the tables columns
     * @param properties           user-defined adapter properties
     */
    public RedshiftTableMetadataReader(final ColumnMetadataReader columnMetadataReader,
            final AdapterProperties properties) {
        super(columnMetadataReader, properties);
    }

    @Override
    public IdentifierCaseHandling getQuotedIdentifierHandling() {
        return IdentifierCaseHandling.INTERPRET_AS_UPPER;
    }
}
