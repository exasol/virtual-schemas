package com.exasol.adapter.dialects.mysql;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect;

public class MySqlSqlDialectFactory {

    public String getSqlDialectName() {
        return MySqlSqlDialect.NAME;
    }

    public SqlDialect createSqlDialect(final Connection connection, final AdapterProperties properties) {
        return new MySqlSqlDialect(connection, properties);
    }
}
