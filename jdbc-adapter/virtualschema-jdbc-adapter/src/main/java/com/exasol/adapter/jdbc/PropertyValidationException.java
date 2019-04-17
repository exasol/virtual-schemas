package com.exasol.adapter.jdbc;

import com.exasol.adapter.AdapterException;

public class PropertyValidationException extends AdapterException {

    public PropertyValidationException(String message) {
        super(message);
    }
}
