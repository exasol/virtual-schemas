package com.exasol.adapter.dialects;

import com.exasol.adapter.AdapterException;

/**
 * This class represents exceptional conditions that occur during validation of the dialect's properties.
 * 
 */
public class PropertyValidationException extends AdapterException {

    /**
     * Create a new {@link PropertyValidationException}
     *
     * @param message error message
     */
    public PropertyValidationException(final String message) {
        super(message);
    }
}
