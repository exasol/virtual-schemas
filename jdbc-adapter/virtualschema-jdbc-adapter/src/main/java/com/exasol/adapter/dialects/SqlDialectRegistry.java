package com.exasol.adapter.dialects;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * This class implements a registry for supported SQL dialects.
 */
public final class SqlDialectRegistry {
    public static final String SQL_DIALECTS_PROPERTY = "com.exasol.adapter.dialects.supported";
    private static final String GET_PUBLIC_NAME_METHOD = "getPublicName";
    private static final String DIALECTS_PROPERTIES_FILE = "sql_dialects.properties";
    private static SqlDialectRegistry instance = null;
    private final Set<Class<? extends SqlDialect>> supportedDialects = new HashSet<>();
    private static final Logger LOGGER = Logger.getLogger(SqlDialectRegistry.class.getName());

    /**
     * Get an instance of the {@link SqlDialectRegistry} class
     *
     * @return the instance
     */
    public static synchronized SqlDialectRegistry getInstance() {
        if (instance == null) {
            instance = new SqlDialectRegistry();
            instance.registerDialectsFromProperty();
        }
        return instance;
    }

    private SqlDialectRegistry() {
        // prevent instantiation outside of singleton.
    }

    private void registerDialectsFromProperty() {
        final String sqlDialects = (System.getProperty(SQL_DIALECTS_PROPERTY) == null)
                ? readDialectListFromPropertyFile()
                : System.getProperty(SQL_DIALECTS_PROPERTY);
        registerDialects(sqlDialects);
    }

    private String readDialectListFromPropertyFile() {
        final Properties properties = new Properties();
        final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try (final InputStream stream = contextClassLoader.getResourceAsStream(DIALECTS_PROPERTIES_FILE)) {
            properties.load(stream);
            return properties.getProperty(SQL_DIALECTS_PROPERTY);
        } catch (final IOException e) {
            throw new SqlDialectRegistryException(
                    "Unable to load list of SQL dialect from " + DIALECTS_PROPERTIES_FILE);
        }
    }

    private void registerDialects(final String sqlDialects) {
        for (final String className : sqlDialects.split("\\s*,\\s*")) {
            registerDialect(className);
        }
    }

    public void registerDialect(final String className) {
        try {
            @SuppressWarnings("unchecked")
            final Class<? extends SqlDialect> dialect = (Class<? extends SqlDialect>) Class.forName(className);
            this.supportedDialects.add(dialect);
            LOGGER.fine(() -> "Registered SQL dialect implementation class \"" + className + "\"");
        } catch (final ClassNotFoundException exception) {
            throw new SqlDialectRegistryException("Unable to find SQL dialect implementation class " + className,
                    exception);
        }
    }

    /**
     * Check whether a dialect is supported
     *
     * @param wantedDialectName the name of the dialect
     * @return <code>true</code> if the dialect is supported
     */
    public boolean isSupported(final String wantedDialectName) {
        return this.supportedDialects.stream().anyMatch(dialect -> {
            return getNameForDialectClass(dialect).equalsIgnoreCase(wantedDialectName);
        });
    }

    private String getNameForDialectClass(final Class<? extends SqlDialect> dialect) {
        final String dialectName;
        try {
            dialectName = (String) dialect.getMethod(GET_PUBLIC_NAME_METHOD).invoke(null);
        } catch (final IllegalAccessException | IllegalArgumentException | InvocationTargetException
                | NoSuchMethodException | SecurityException e) {
            throw new SqlDialectRegistryException(
                    "Unable to invoke " + GET_PUBLIC_NAME_METHOD + " trying to determine SQL dialect name");
        }
        return dialectName;
    }

    private Optional<Class<? extends SqlDialect>> findDialectByName(final String name) {
        return this.supportedDialects.stream() //
                .filter(dialect -> getNameForDialectClass(dialect).equalsIgnoreCase(name)) //
                .findFirst();
    }

    /**
     * Get a comma separated, alphabetically sorted list of supported dialects.
     *
     * @return comma separated list of dialect.
     */
    public String getDialectsString() {
        return this.supportedDialects.stream() //
                .map(dialect -> getNameForDialectClass(dialect)) //
                .sorted() //
                .collect(Collectors.joining(", "));
    }

    /**
     * Delete the singleton instance (necessary for tests)
     */
    public static void deleteInstance() {
        instance = null;
    }

    /**
     * Get SQL dialect class for name
     *
     * @param name name of the SQL dialect
     * @return subclass of {@link SqlDialect} according to the name
     */
    public Class<? extends SqlDialect> getSqlDialectClassForName(final String name) {
        final Optional<Class<? extends SqlDialect>> foundDialect = findDialectByName(name);
        if (foundDialect.isPresent()) {
            return foundDialect.get();
        } else {
            throw new IllegalArgumentException("Could not find dialect with the name \"" + name + "\".");
        }
    }
}