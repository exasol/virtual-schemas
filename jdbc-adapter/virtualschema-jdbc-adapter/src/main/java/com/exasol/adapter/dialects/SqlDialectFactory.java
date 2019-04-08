package com.exasol.adapter.dialects;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;

public class SqlDialectFactory {
    private final SqlDialectRegistry sqlDialectRegistry;
    private final AdapterProperties properties;
    private final Connection connection;

    public SqlDialectFactory(final Connection connection, final SqlDialectRegistry sqlDialectRegistry,
            final AdapterProperties properties) {
        this.connection = connection;
        this.sqlDialectRegistry = sqlDialectRegistry;
        this.properties = properties;
    }

    public SqlDialect createSqlDialect(final String dialectName) {
        final Class<? extends SqlDialect> sqlDialectClass = this.sqlDialectRegistry
                .getSqlDialectClassForName(dialectName);
        return instantiateDialect(sqlDialectClass, dialectName);
    }

    private SqlDialect instantiateDialect(final Class<? extends SqlDialect> dialectClass, final String dialectName) {
        try {
            return dialectClass.getConstructor(Connection.class, AdapterProperties.class).newInstance(this.connection,
                    this.properties);
        } catch (final InstantiationException | IllegalAccessException | InvocationTargetException
                | NoSuchMethodException exception) {
            throw new SqlDialectFactoryException("Unable to instantiate SQL dialect \"" + dialectName + "\".",
                    exception);
        }
    }
}