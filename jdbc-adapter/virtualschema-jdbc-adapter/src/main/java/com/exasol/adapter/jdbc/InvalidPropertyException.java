package com.exasol.adapter.jdbc;

import com.exasol.adapter.AdapterException;

public class InvalidPropertyException extends AdapterException{

    public InvalidPropertyException(String message) {
        super(message);
    }
}
