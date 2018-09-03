package com.exasol.adapter.dialects;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The {@link SqlDialectLoader} scans the class path for SQL dialect drivers.
 */
public final class SqlDialectLoader {
    private static final String ALL_RESOURCES = "";
    private static final Pattern DIALECT_FILENAME_PATTERN = Pattern.compile("(\\w+SqlDialect)\\.class");
    private static SqlDialectLoader instance = null;

    private SqlDialectLoader() {
    }

    /**
     * Get a singleton instance of the {@link SqlDialectLoader}
     *
     * @return {@link SqlDialectLoader} instance
     */
    public static synchronized SqlDialectLoader getInstance() {
        if (SqlDialectLoader.instance == null) {
            SqlDialectLoader.instance = new SqlDialectLoader();
        }
        return SqlDialectLoader.instance;
    }

    /**
     * Scan the class path for classes that follow the naming convention for SQL
     * dialects.
     *
     * @return Set of classes matching the naming convention
     */
    public Set<String> findDriverClassNames() {
        final ClassLoader classLoader = this.getClass().getClassLoader();
        try {
            final Enumeration<URL> roots = classLoader.getResources(ALL_RESOURCES);
            final Set<String> dialectNames = new HashSet<>();
            while (roots.hasMoreElements()) {
                final File element = new File(roots.nextElement().getPath());
                collectDialectFilesRecursive(element, dialectNames);
            }
            return dialectNames;
        } catch (final IOException e) {
            throw new SqlDialectLoaderException("Unable to scan for SQL dialect drivers", e);
        }
    }

    private void collectDialectFilesRecursive(final File element, final Set<String> dialectNames) {
        if (element.isDirectory()) {
            for (final File subElement : element.listFiles()) {
                collectDialectFilesRecursive(subElement, dialectNames);
            }
        } else {
            final String name = element.getName();
            final Matcher matcher = DIALECT_FILENAME_PATTERN.matcher(name);
            if (matcher.matches() && !name.startsWith("Abstract") && !name.contains("Dummy")) {
                dialectNames.add(matcher.group(1));
            }
        }
    }

    /**
     * Finds all SQL dialect classes that implement {@link SqlDialect}.
     *
     * @return set of SQL dialect classes
     */
    public Set<Class<? extends SqlDialect>> findDriverClasses() {
        final Set<Class<? extends SqlDialect>> dialectClasses = new HashSet<>();
        for (final String className : findDriverClassNames()) {
            try {
                final Class<?> foundClass = Class.forName("com.exasol.adapter.dialects.impl." + className);
                if (SqlDialect.class.isAssignableFrom(foundClass)) {
                    @SuppressWarnings("unchecked")
                    final Class<? extends SqlDialect> castedClass = (Class<? extends SqlDialect>) foundClass;
                    dialectClasses.add(castedClass);
                }
            } catch (final ClassNotFoundException e) {
                throw new SqlDialectLoaderException(
                        "Unable to get SQL dialect driver class for name \"" + className + "\"", e);
            }
        }
        return dialectClasses;
    }
}