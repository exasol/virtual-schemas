package com.exasol.adapter.dialects.dummy;

import com.exasol.adapter.dialects.AbstractSqlDialectFactory;
import com.exasol.adapter.dialects.SqlDialect;

public class DummySqlDialectFactory extends AbstractSqlDialectFactory {
    private static final String DIALECT_NAME = "DUMMYDIALECT";

    @Override
    public String getSqlDialectName() {
        return DIALECT_NAME;
    }

    @Override
    protected Class<? extends SqlDialect> getSqlDialectClass() {
        return DummySqlDialect.class;
    }
}