package com.exasol.adapter.dialects.sybase;

import java.util.Objects;
import java.util.Set;

import com.exasol.db.Identifier;

/**
 * Represents an identifier in the Sybase database.
 */
public class SybaseIdentifier implements Identifier {
    private static final Set<Character> ALLOWED_CHARS = Set.of('_', '@', '#', '$', '¥', '£');
    private final String id;

    private SybaseIdentifier(final String id) {
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
     * Create a new {@link SybaseIdentifier}.
     *
     * @param id the identifier as {@link String}
     * @return new {@link SybaseIdentifier} instance
     */
    public static SybaseIdentifier of(final String id) {
        if (validate(id)) {
            return new SybaseIdentifier(id);
        } else {
            throw new AssertionError("E-ID-5: Unable to create identifier \"" + id //
                    + "\" because it contains illegal characters." //
                    + " For information about valid identifiers, please refer to" //
                    + " http://infocenter.sybase.com/help/index.jsp?topic=/com.sybase.infocenter.dc36271.1600/doc/html/san1393050529478.html");
        }
    }

    private static boolean validate(final String id) {
        if ((id == null) || id.isEmpty()) {
            return false;
        }
        for (int i = 0; i < id.length(); ++i) {
            if (!validateCharacter(id.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean validateCharacter(final char ch) {
        return ALLOWED_CHARS.contains(ch) || Character.isDigit(ch) || (ch >= 'a' && ch <= 'z')
                || (ch >= 'A' && ch <= 'Z');
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SybaseIdentifier)) {
            return false;
        }
        final SybaseIdentifier that = (SybaseIdentifier) o;
        return Objects.equals(this.id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.id);
    }
}