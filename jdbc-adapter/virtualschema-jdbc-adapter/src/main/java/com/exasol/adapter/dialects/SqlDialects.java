package com.exasol.adapter.dialects;

import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class implements a registry for supported SQL dialects.
 */
public final class SqlDialects {
    private static final String GET_PUBLIC_NAME_METHOD = "getPublicName";
    private static SqlDialects instance;
    private final Set<Class<? extends SqlDialect>> supportedDialects = new HashSet<>();

    private SqlDialects() {
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
        String dialectName;
        try {
            dialectName = (String) dialect.getMethod(GET_PUBLIC_NAME_METHOD).invoke(null);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
                | SecurityException e) {
            throw new RuntimeException(
                    "Unable to invoke " + GET_PUBLIC_NAME_METHOD + " trying to determine SQL dialect name");
        }
        return dialectName;
    }

    /**
     * Finds an SQL dialect by its name and hands back an instance of the according
     * dialect implementation.
     *
     * @param name    name of the dialect to be instanciated
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
        SqlDialect instance;
        try {
            final Class<? extends SqlDialect> dialectClass = foundDialect.get();
            instance = dialectClass.getConstructor(SqlDialectContext.class).newInstance(context);
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
                | NoSuchMethodException | SecurityException e) {
            throw new SqlDialectsRegistryException("Unable to instanciate SQL dialect \"" + name + "\".", e);
        }
        return instance;
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
     * Get an instance of the {@link SqlDialects} class
     *
     * @return the instance
     */
    public static synchronized SqlDialects getInstance() {
        if (instance == null) {
            instance = new SqlDialects();
        }
        return instance;
    }

    /**
     * Register a new dialect
     *
     * @param dialect the dialect to register
     */
    public void register(final Class<? extends SqlDialect> dialect) {
        this.supportedDialects.add(dialect);
    }

    /**
     * Register a set of dialects
     *
     * @param dialects the set of dialects to register
     */
    public void registerAll(final Set<Class<? extends SqlDialect>> dialects) {
        this.supportedDialects.addAll(dialects);
    }
}