package com.exasol.adapter.dialects;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.util.logging.Logger;

import com.exasol.adapter.AdapterProperties;

/**
 * This class implements a factory that instantiates the right SQL dialect adapter.
 */
public class SqlDialectFactory {
    private static final Logger LOGGER = Logger.getLogger(SqlDialectFactory.class.getName());
    private final SqlDialectRegistry sqlDialectRegistry;
    private final AdapterProperties properties;
    private final Connection connection;

    /**
     * Create a new instance of an {@link SqlDialectFactory}.
     *
     * @param connection         JDBC connection to the remote data source
     * @param sqlDialectRegistry registry where the available dialect implementations are listed
     * @param properties         user-defined adapter properties
     */
    public SqlDialectFactory(final Connection connection, final SqlDialectRegistry sqlDialectRegistry,
            final AdapterProperties properties) {
        this.connection = connection;
        this.sqlDialectRegistry = sqlDialectRegistry;
        this.properties = properties;
    }

    /**
     * Create an instance of the SQL dialect adapter matching the dialect name.
     *
     * @param dialectName name of the SQL dialect for which the factory should instantiate an adapter
     * @return SQL dialect adapter
     */
    public SqlDialect createSqlDialect(final String dialectName) {
        final Class<? extends SqlDialect> sqlDialectClass = this.sqlDialectRegistry
                .getSqlDialectClassForName(dialectName);
        return instantiateDialect(sqlDialectClass, dialectName);
    }

    private SqlDialect instantiateDialect(final Class<? extends SqlDialect> dialectClass, final String dialectName) {
        try {
            LOGGER.fine(() -> "Instantiating SQL dialect class " + dialectClass.getName());
            return dialectClass.getConstructor(Connection.class, AdapterProperties.class).newInstance(this.connection,
                    this.properties);
        } catch (final InstantiationException | IllegalAccessException | InvocationTargetException
                | NoSuchMethodException exception) {
            throw new SqlDialectFactoryException("Unable to instantiate SQL dialect \"" + dialectName + "\".",
                    exception);
        }
    }
}