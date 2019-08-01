package com.exasol.adapter.dialects.saphana;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlDialectFactory;

/**
 * Factory for the HANA dialect
 */
public class SapHanaSqlDialectFactory implements SqlDialectFactory {
    @Override
    public String getSqlDialectName() {
        return SapHanaSqlDialect.NAME;
    }

    @Override
    public SqlDialect createSqlDialect(final Connection connection, final AdapterProperties properties) {
        return new SapHanaSqlDialect(connection, properties);
    }
}