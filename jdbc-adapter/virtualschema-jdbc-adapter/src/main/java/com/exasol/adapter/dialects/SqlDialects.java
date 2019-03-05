package com.exasol.adapter.dialects;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * This class implements a registry for supported SQL dialects.
 */
public final class SqlDialects {
    public static final String SQL_DIALECTS_PROPERTY = "com.exasol.adapter.dialects.supported";
    private static final String GET_PUBLIC_NAME_METHOD = "getPublicName";
    private static final String DIALECTS_PROPERTIES_FILE = "sql_dialects.properties";
    private static SqlDialects instance = null;
    private final Set<Class<? extends SqlDialect>> supportedDialects = new HashSet<>();
    private static final Logger LOGGER = Logger.getLogger(SqlDialects.class.getName());

    /**
     * Get an instance of the {@link SqlDialects} class
     *
     * @return the instance
     */
    public static synchronized SqlDialects getInstance() {
        if (instance == null) {
            instance = new SqlDialects();
            instance.registerDialectsFromProperty();
        }
        return instance;
    }

    private SqlDialects() {
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
            throw new SqlDialectsRegistryException(
                    "Unable to load list of SQL dialect from " + DIALECTS_PROPERTIES_FILE);
        }
    }

    private void registerDialects(final String sqlDialects) {
        for (final String className : sqlDialects.split("\\s*,\\s*")) {
            registerDialect(className);
        }
    }

    private void registerDialect(final String className) {
        try {
            @SuppressWarnings("unchecked")
            final Class<? extends SqlDialect> dialect = (Class<? extends SqlDialect>) Class.forName(className);
            this.supportedDialects.add(dialect);
            LOGGER.fine(() -> "Registered SQL dialect implementation class \"" + className + "\"");
        } catch (final ClassNotFoundException e) {
            throw new SqlDialectsRegistryException("Unable to find SQL dialect implementation class " + className);
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
        } catch (final IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
                | SecurityException e) {
            throw new SqlDialectsRegistryException(
                    "Unable to invoke " + GET_PUBLIC_NAME_METHOD + " trying to determine SQL dialect name");
        }
        return dialectName;
    }

    /**
     * Finds an SQL dialect by its name and hands back an instance of the according
     * dialect implementation.
     *
     * @param name    name of the dialect to be instantiated
     * @param context the context to be handed to the instance.
     * @return a new instance of the dialect
     *
     * @throws SqlDialectsRegistryException if the dialect is not found or cannot be
     *                                      instantiated.
     */
    public SqlDialect getDialectInstanceForNameWithContext(final String name, final SqlDialectContext context)
            throws SqlDialectsRegistryException {
        final Optional<Class<? extends SqlDialect>> foundDialect = findDialectByName(name);
        return instantiateDialect(name, foundDialect, context);
    }

    private Optional<Class<? extends SqlDialect>> findDialectByName(final String name) {
        final Optional<Class<? extends SqlDialect>> foundDialect = this.supportedDialects.stream()
                .filter(dialect -> getNameForDialectClass(dialect).equalsIgnoreCase(name)) //
                .findFirst();
        if (!foundDialect.isPresent()) {
            throw new SqlDialectsRegistryException("SQL dialect \"" + name + "\" not found in the dialects registry.");
        }
        return foundDialect;
    }

    private SqlDialect instantiateDialect(final String name, final Optional<Class<? extends SqlDialect>> foundDialect,
          final SqlDialectContext context) throws SqlDialectsRegistryException {
        if (foundDialect.isPresent()) {
            try {
                final Class<? extends SqlDialect> dialectClass = foundDialect.get();
                return dialectClass.getConstructor(SqlDialectContext.class).newInstance(context);
            } catch (final InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
                throw new SqlDialectsRegistryException("Unable to instanciate SQL dialect \"" + name + "\".", e);
            }
        } else {
            throw new SqlDialectsRegistryException("The dialect \"" + name + "\" does not exist."//
                  + "Please, select the dialect from the next list: " + getDialectsString());
        }
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
}