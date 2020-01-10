package com.exasol.reflect;

/**
 * This class is intended for wrapping exceptions thrown during attempts to use reflection to keep the number of
 * declared exceptions down in unit tests.
 */
public final class ReflectionException extends RuntimeException {
    private static final long serialVersionUID = 63504710445197156L;

    public ReflectionException(final Throwable cause) {
        super(cause);
    }
}