package com.exasol.logging;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**
 * Formatter for compact log messages.
 */
public class CompactFormatter extends Formatter {
    private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    @Override
    public String format(final LogRecord record) {
        final StringBuilder builder = new StringBuilder();
        final Instant instant = Instant.ofEpochMilli(record.getMillis());
        builder.append(this.dateTimeFormatter.format(instant));
        builder.append(String.format("%74s", record.getLevel()));
        builder.append(" [");
        builder.append(record.getSourceClassName());
        builder.append("] ");
        builder.append(record.getMessage());
        return builder.toString();
    }
}