package com.exasol.adapter.dialects;

public enum PostgreSQLIdentifierMapping {
    CONVERT_TO_UPPER, PRESERVE_ORIGINAL_CASE;

    public static PostgreSQLIdentifierMapping parse(final String mapping) {
        switch (mapping) {
        case "CONVERT_TO_UPPER":
            return CONVERT_TO_UPPER;
        case "PRESERVE_ORIGINAL_CASE":
            return PRESERVE_ORIGINAL_CASE;
        default:
            throw new IllegalArgumentException("Unable to parse PostgreSQL identifier mapping \"" + mapping + "\"");
        }
    }
}
