package com.exasol.adapter.dialects.impala;

import java.util.Objects;

import com.exasol.db.Identifier;

/**
 * Represents an identifier in the Impala database.
 */
public class ImpalaIdentifier implements Identifier {
    private final String id;

    private ImpalaIdentifier(final String id) {
        this.id = id;
    }

    /**
     * Get the quoted identifier as a {@link String}.
     *
     * @return quoted identifier
     */
    @Override
    public String quote() {
        return "`" + this.id + "`";
    }

    /**
     * Create a new {@link ImpalaIdentifier}.
     *
     * @param id the identifier as {@link String}
     * @return new {@link ImpalaIdentifier} instance
     */
    public static ImpalaIdentifier of(final String id) {
        if (validate(id)) {
            return new ImpalaIdentifier(id);
        } else {
            throw new AssertionError("E-ID-6: Unable to create identifier \"" + id //
                    + "\" because it contains illegal characters." //
                    + " For information about valid identifiers, please refer to" //
                    + " https://docs.cloudera.com/documentation/enterprise/latest/topics/impala_identifiers.html");
        }
    }

    private static boolean validate(final String id) {
        return !id.contains("`");
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ImpalaIdentifier)) {
            return false;
        }
        final ImpalaIdentifier that = (ImpalaIdentifier) o;
        return Objects.equals(this.id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.id);
    }
}
