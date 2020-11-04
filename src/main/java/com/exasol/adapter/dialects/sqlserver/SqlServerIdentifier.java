package com.exasol.adapter.dialects.sqlserver;

import java.util.Objects;

import com.exasol.db.Identifier;

/**
 * Represents an identifier in the Sql Server database.
 */
public class SqlServerIdentifier implements Identifier {
    private final String id;

    private SqlServerIdentifier(final String id) {
        this.id = id;
    }

    /**
     * Get the quoted identifier as a {@link String}.
     *
     * @return quoted identifier
     */
    @Override
    public String quote() {
        return "[" + this.id + "]";
    }

    /**
     * Create a new {@link SqlServerIdentifier}.
     *
     * @param id the identifier as {@link String}
     * @return new {@link SqlServerIdentifier} instance
     */
    public static SqlServerIdentifier of(final String id) {
        if (validate(id)) {
            return new SqlServerIdentifier(id);
        } else {
            throw new AssertionError("E-ID-3: Unable to create identifier \"" + id //
                    + "\" because it contains illegal characters." //
                    + " For information about valid identifiers, please refer to" //
                    + " https://docs.microsoft.com/en-us/sql/relational-databases/databases/database-identifiers?redirectedfrom=MSDN&view=sql-server-ver15");
        }
    }

    private static boolean validate(final String id) {
        return !id.contains("[") && !id.contains("]");
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        final SqlServerIdentifier that = (SqlServerIdentifier) o;
        return Objects.equals(this.id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.id);
    }
}