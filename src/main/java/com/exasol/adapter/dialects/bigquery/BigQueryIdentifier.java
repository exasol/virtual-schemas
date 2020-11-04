package com.exasol.adapter.dialects.bigquery;

import java.util.Objects;

import com.exasol.db.Identifier;

/**
 * Represents an identifier in the BigQuery database.
 */
public class BigQueryIdentifier implements Identifier {
    private final String id;

    private BigQueryIdentifier(final String id) {
        this.id = id;
    }

    /**
     * Get the quoted identifier as a {@link String}.
     *
     * @return quoted identifier
     */
    @Override
    public String quote() {
        return "`" + this.id.replace("`", "\\`") + "`";
    }

    /**
     * Create a new {@link BigQueryIdentifier}.
     * <p>
     * BigQuery allows any characters when the identifier is quoted. We don't have validations here.
     * https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical
     * </p>
     *
     * @param id the identifier as {@link String}
     * @return new {@link BigQueryIdentifier} instance
     */
    public static BigQueryIdentifier of(final String id) {
        return new BigQueryIdentifier(id);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        final BigQueryIdentifier that = (BigQueryIdentifier) o;
        return Objects.equals(this.id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.id);
    }
}