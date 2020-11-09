package com.exasol.adapter.dialects.oracle;

import java.util.Objects;

import com.exasol.db.Identifier;

/**
 * Represents an identifier in the Oracle database.
 */
public class OracleIdentifier implements Identifier {
    private final String id;

    private OracleIdentifier(final String id) {
        this.id = id;
    }

    /**
     * Get the quoted identifier as a {@link String}.
     *
     * @return quoted identifier
     */
    @Override
    public String quote() {
        return "\"" + this.id + "\"";
    }

    /**
     * Create a new {@link OracleIdentifier}.
     *
     * @param id the identifier as {@link String}
     * @return new {@link OracleIdentifier} instance
     */
    public static OracleIdentifier of(final String id) {
        if (validate(id)) {
            return new OracleIdentifier(id);
        } else {
            throw new AssertionError("E-ID-3: Unable to create identifier \"" + id //
                    + "\" because it contains illegal characters." //
                    + " For information about valid identifiers, please refer to" //
                    + " https://docs.oracle.com/cd/B19306_01/server.102/b14200/sql_elements008.htm");
        }
    }

    private static boolean validate(final String id) {
        return !id.contains("\"");
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OracleIdentifier)) {
            return false;
        }
        final OracleIdentifier that = (OracleIdentifier) o;
        return Objects.equals(this.id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.id);
    }
}