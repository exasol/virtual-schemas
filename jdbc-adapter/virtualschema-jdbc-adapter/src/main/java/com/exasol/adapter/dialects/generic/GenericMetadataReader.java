package com.exasol.adapter.dialects.generic;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.*;

/**
 * This class reads Generic database metadata
 */
public class GenericMetadataReader extends BaseRemoteMetadataReader {
    /**
     * Create a new instance of a {@link GenericMetadataReader}
     *
     * @param connection database connection through which the reader retrieves the metadata from the remote source
     * @param properties user-defined properties
     */
    public GenericMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    // FIXME these methods should be in Column and Table MetadataReaders. but they have adapterNotes dependency. They
    // don't work here.
    @Override
    public IdentifierCaseHandling getUnquotedIdentifierHandling() {
        final SchemaAdapterNotes adapterNotes = getSchemaAdapterNotes();
        if (adapterNotes.supportsMixedCaseIdentifiers()) {
            return IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE;
        } else {
            if (adapterNotes.storesLowerCaseIdentifiers()) {
                return IdentifierCaseHandling.INTERPRET_AS_LOWER;
            } else if (adapterNotes.storesUpperCaseIdentifiers()) {
                return IdentifierCaseHandling.INTERPRET_AS_UPPER;
            } else if (adapterNotes.storesMixedCaseIdentifiers()) {
                return IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE;
            } else {
                throw new UnsupportedOperationException("Unexpected quote behavior. Adapter notes: " //
                        + adapterNotes.toString());
            }
        }
    }

    @Override
    public IdentifierCaseHandling getQuotedIdentifierHandling() {
        final SchemaAdapterNotes adapterNotes = getSchemaAdapterNotes();
        if (adapterNotes.supportsMixedCaseQuotedIdentifiers()) {
            return IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE;
        } else {
            if (adapterNotes.storesLowerCaseQuotedIdentifiers()) {
                return IdentifierCaseHandling.INTERPRET_AS_LOWER;
            } else if (adapterNotes.storesUpperCaseQuotedIdentifiers()) {
                return IdentifierCaseHandling.INTERPRET_AS_UPPER;
            } else if (adapterNotes.storesMixedCaseQuotedIdentifiers()) {
                return IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE;
            } else {
                throw new UnsupportedOperationException("Unexpected quote behavior. Adapter notes: " //
                        + adapterNotes.toString());
            }
        }
    }
}
