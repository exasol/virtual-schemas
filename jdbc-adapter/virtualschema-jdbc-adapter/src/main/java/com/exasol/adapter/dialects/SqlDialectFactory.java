package com.exasol.adapter.dialects;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.util.Map;

public class SqlDialectFactory {
    private final Connection connection;
    private final SqlDialectRegistry sqlDialectRegistry;
    private final Map<String, String> properties;

    public SqlDialectFactory(final Connection connection, final SqlDialectRegistry sqlDialectRegistry,
          final Map<String, String> properties) {
        this.connection = connection;
        this.sqlDialectRegistry = sqlDialectRegistry;
        this.properties = properties;
    }

    public SqlDialect createSqlDialect(final String dialectName) {
        final Class<? extends SqlDialect> sqlDialectClass =
              this.sqlDialectRegistry.getSqlDialectClassForName(dialectName);
        return instantiateDialect(sqlDialectClass, dialectName);
    }

    private SqlDialect instantiateDialect(final Class<? extends SqlDialect> dialectClass, final String dialectName) {
        try {
            return dialectClass.getConstructor()
                  .newInstance(); //FIXME: instantiate with correct parameters
        } catch (final InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException exception) {
            throw new SqlDialectFactoryException("Unable to instantiate SQL dialect \"" + dialectName + "\".",
                  exception);
        }
    }
}