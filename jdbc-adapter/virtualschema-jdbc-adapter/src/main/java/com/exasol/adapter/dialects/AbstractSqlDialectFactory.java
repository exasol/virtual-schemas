package com.exasol.adapter.dialects;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.util.logging.Logger;

import com.exasol.adapter.AdapterProperties;

public abstract class AbstractSqlDialectFactory implements SqlDialectFactory {
    protected static final Logger LOGGER = Logger.getLogger(AbstractSqlDialectFactory.class.getName());

    @Override
    public SqlDialect createSqlDialect(final Connection connection, final AdapterProperties properties) {
        return instantiateDialect(getSqlDialectClass(), connection, properties);
    }

    protected abstract Class<? extends SqlDialect> getSqlDialectClass();

    private SqlDialect instantiateDialect(final Class<? extends SqlDialect> dialectClass, final Connection connection,
            final AdapterProperties properties) {
        try {
            LOGGER.fine(() -> "Instantiating SQL dialect class " + dialectClass.getName());
            return dialectClass.getConstructor(Connection.class, AdapterProperties.class).newInstance(connection,
                    properties);
        } catch (final InstantiationException | IllegalAccessException | InvocationTargetException
                | NoSuchMethodException exception) {
            throw new SqlDialectFactoryException("Unable to instantiate SQL dialect \"" + getSqlDialectName() + "\".",
                    exception);
        }
    }
}