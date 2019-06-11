package com.exasol.adapter.sql;

import com.exasol.adapter.AdapterException;

public class DummySqlStatement extends SqlStatement {

    private static final String DUMMY_SELECT = "SELECT 1 FROM DUAL";

    @Override
    public SqlNodeType getType() {
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <R> R accept(final SqlNodeVisitor<R> visitor) throws AdapterException {
        return (R) DUMMY_SELECT;
    }

    @Override
    String toSimpleSql() {
        return DUMMY_SELECT;
    }
}