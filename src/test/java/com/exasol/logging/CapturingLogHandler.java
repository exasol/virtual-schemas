package com.exasol.logging;

import java.util.logging.Handler;
import java.util.logging.LogRecord;

/**
 * This class implements a dummy log handler that allows capturing the log messages.
 * <p>
 * This allows checking the contents of the log messages in unit tests.
 */
public class CapturingLogHandler extends Handler {
    private final StringBuilder buffer = new StringBuilder();

    @Override
    public void publish(final LogRecord record) {
        this.buffer.append(record.getMessage());
    }

    @Override
    public void flush() {
        // intentionally blank
    }

    @Override
    public void close() throws SecurityException {
        reset();
    }

    /**
     * Get the captured data from the messages
     * 
     * @return messages as string
     */
    public String getCapturedData() {
        return (this.buffer == null) ? null : this.buffer.toString();
    }

    /**
     * Reset the capture buffer
     */
    public void reset() {
        this.buffer.setLength(0);
    }
}