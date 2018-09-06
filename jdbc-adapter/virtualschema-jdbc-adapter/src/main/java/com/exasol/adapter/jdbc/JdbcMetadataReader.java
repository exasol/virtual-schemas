package com.exasol.adapter.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlDialectContext;
import com.exasol.adapter.dialects.SqlDialects;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.metadata.TableMetadata;
import com.google.common.base.Joiner;

/**
 * TODO Find good solutions to handle tables with unsupported data types, or
 * tables that generate exceptions. Ideas: Skip such tables by adding a boolean
 * property like IGNORE_INVALID_TABLES.
 */
public class JdbcMetadataReader {

    public static SchemaMetadata readRemoteMetadata(final String connectionString, final String user,
            final String password, String catalog, String schema, final List<String> tableFilter,
            final String dialectName, final JdbcAdapterProperties.ExceptionHandlingMode exceptionMode)
            throws SQLException, AdapterException {
        assert (catalog != null);
        assert (schema != null);
        try {
            final Connection conn = establishConnection(connectionString, user, password);
            final DatabaseMetaData dbMeta = conn.getMetaData();

            // Retrieve relevant parts of DatabaseMetadata. Will be cached in adapternotes
            // of the schema.
            final SchemaAdapterNotes schemaAdapterNotes = new SchemaAdapterNotes(dbMeta.getCatalogSeparator(),
                    dbMeta.getIdentifierQuoteString(), dbMeta.storesLowerCaseIdentifiers(),
                    dbMeta.storesUpperCaseIdentifiers(), dbMeta.storesMixedCaseIdentifiers(),
                    dbMeta.supportsMixedCaseIdentifiers(), dbMeta.storesLowerCaseQuotedIdentifiers(),
                    dbMeta.storesUpperCaseQuotedIdentifiers(), dbMeta.storesMixedCaseQuotedIdentifiers(),
                    dbMeta.supportsMixedCaseQuotedIdentifiers(), dbMeta.nullsAreSortedAtEnd(),
                    dbMeta.nullsAreSortedAtStart(), dbMeta.nullsAreSortedHigh(), dbMeta.nullsAreSortedLow());

            final SqlDialect dialect = SqlDialects.getInstance().getDialectInstanceForNameWithContext(dialectName,
                    new SqlDialectContext(schemaAdapterNotes));

            catalog = findCatalog(catalog, dbMeta, dialect);

            schema = findSchema(schema, dbMeta, dialect);

            final List<TableMetadata> tables = findTables(catalog, schema, tableFilter, dbMeta, dialect, exceptionMode);

            conn.close();
            return new SchemaMetadata(SchemaAdapterNotes.serialize(schemaAdapterNotes), tables);
        } catch (final SQLException e) {
            e.printStackTrace();
            throw e;
        }
    }

    private static Connection establishConnection(final String connectionString, final String user,
            final String password) throws SQLException {
        System.out.println("conn: " + connectionString);

        final java.util.Properties info = new java.util.Properties();
        if (user != null) {
            info.put("user", user);
        }
        if (password != null) {
            info.put("password", password);
        }
        if (KerberosUtils.isKerberosAuth(password)) {
            try {
                KerberosUtils.configKerberos(user, password);
            } catch (final Exception e) {
                e.printStackTrace();
                throw new RuntimeException("Error configuring Kerberos: " + e.getMessage(), e);
            }
        }
        return DriverManager.getConnection(connectionString, info);
    }

    private static String findCatalog(final String catalog, final DatabaseMetaData dbMeta, final SqlDialect dialect)
            throws SQLException, AdapterException {
        boolean foundCatalog = false;
        String curCatalog = "";
        int numCatalogs = 0;
        final List<String> allCatalogs = new ArrayList<>();
        ResultSet res = null;
        try {
            res = dbMeta.getCatalogs();
            while (res.next()) {
                curCatalog = res.getString("TABLE_CAT"); // EXA_DB in case of EXASOL
                allCatalogs.add(curCatalog);
                if (curCatalog.equals(catalog)) {
                    foundCatalog = true;
                }
                ++numCatalogs;
            }
        } catch (final Exception ex) {
            if (dialect.supportsJdbcCatalogs() == SqlDialect.SchemaOrCatalogSupport.SUPPORTED) {
                throw new RuntimeException("Unexpected exception when accessing the catalogs: " + ex.getMessage(), ex);
            } else if (dialect.supportsJdbcCatalogs() == SqlDialect.SchemaOrCatalogSupport.UNSUPPORTED) {
                // Ignore this error
                ex.printStackTrace();
                return null;
            } else {
                // We don't know if system supports catalogs. If user specified an catalog, we
                // have a problem, otherwise we ignore the error
                if (!catalog.isEmpty()) {
                    throw new RuntimeException("Unexpected exception when accessing the catalogs: " + ex.getMessage(),
                            ex);
                } else {
                    ex.printStackTrace();
                    return null;
                }
            }
        } finally {
            if (res != null) {
                res.close();
            }
        }
        if (dialect.supportsJdbcCatalogs() == SqlDialect.SchemaOrCatalogSupport.SUPPORTED
                || dialect.supportsJdbcCatalogs() == SqlDialect.SchemaOrCatalogSupport.UNKNOWN) {
            if (foundCatalog) {
                return catalog;
            } else {
                if (catalog.isEmpty()) {
                    if (dialect.supportsJdbcCatalogs() == SqlDialect.SchemaOrCatalogSupport.SUPPORTED) {
                        throw new AdapterException("You have to specify a catalog. Available catalogs: "
                                + Joiner.on(", ").join(allCatalogs));
                    } else {
                        if (numCatalogs == 0) {
                            return null;
                        } else {
                            throw new AdapterException("You have to specify a catalog. Available catalogs: "
                                    + Joiner.on(", ").join(allCatalogs));
                        }
                    }
                } else {
                    throw new AdapterException("Catalog " + catalog + " does not exist. Available catalogs: "
                            + Joiner.on(", ").join(allCatalogs));
                }
            }
        } else {
            assert (dialect.supportsJdbcCatalogs() == SqlDialect.SchemaOrCatalogSupport.UNSUPPORTED);
            if (catalog.isEmpty()) {
                if (numCatalogs == 0) {
                    return null;
                } else if (numCatalogs == 1) {
                    // Take the one and only catalog (in case of EXASOL this is always EXA_DB).
                    // Returning null would probably also work fine.
                    return curCatalog;
                } else {
                    throw new AdapterException("The data source is not expected to support catalogs, but has "
                            + numCatalogs + " catalogs: " + Joiner.on(", ").join(allCatalogs));
                }
            } else {
                throw new AdapterException(
                        "You specified a catalog, however the data source does not support the concept of catalogs.");
            }
        }
    }

    private static String findSchema(final String schema, final DatabaseMetaData dbMeta, final SqlDialect dialect)
            throws SQLException, AdapterException {
        // Check if schema exists
        boolean foundSchema = false;
        final List<String> allSchemas = new ArrayList<>();
        int numSchemas = 0;
        String curSchema = "";
        ResultSet schemas = null;

        try {
            schemas = dbMeta.getSchemas();
            while (schemas.next()) {
                curSchema = schemas.getString("TABLE_SCHEM");
                allSchemas.add(curSchema);
                if (curSchema.equals(schema)) {
                    foundSchema = true;
                }
                ++numSchemas;
            }
        } catch (final Exception ex) {
            if (dialect.supportsJdbcSchemas() == SqlDialect.SchemaOrCatalogSupport.SUPPORTED) {
                throw new RuntimeException("Unexpected exception when accessing the schema: " + ex.getMessage(), ex);
            } else if (dialect.supportsJdbcSchemas() == SqlDialect.SchemaOrCatalogSupport.UNSUPPORTED) {
                // Ignore this error
                ex.printStackTrace();
                return null;
            } else {
                // We don't know if system supports schemas.
                if (!schema.isEmpty()) {
                    throw new RuntimeException("Unexpected exception when accessing the schemas: " + ex.getMessage(),
                            ex);
                } else {
                    ex.printStackTrace();
                    return null;
                }
            }
        } finally {
            if (schemas != null) {
                schemas.close();
            }
        }

        if (dialect.supportsJdbcSchemas() == SqlDialect.SchemaOrCatalogSupport.SUPPORTED
                || dialect.supportsJdbcSchemas() == SqlDialect.SchemaOrCatalogSupport.UNKNOWN) {
            if (foundSchema) {
                return schema;
            } else {
                if (schema.isEmpty()) {
                    if (dialect.supportsJdbcSchemas() == SqlDialect.SchemaOrCatalogSupport.SUPPORTED) {
                        throw new AdapterException(
                                "You have to specify a schema. Available schemas: " + Joiner.on(", ").join(allSchemas));
                    } else {
                        if (numSchemas == 0) {
                            return null;
                        } else {
                            throw new AdapterException("You have to specify a schema. Available schemas: "
                                    + Joiner.on(", ").join(allSchemas));
                        }
                    }
                } else {
                    throw new AdapterException("Schema " + schema + " does not exist. Available schemas: "
                            + Joiner.on(", ").join(allSchemas));
                }
            }
        } else {
            assert (dialect.supportsJdbcSchemas() == SqlDialect.SchemaOrCatalogSupport.UNSUPPORTED);
            if (schema.isEmpty()) {
                if (numSchemas == 0) {
                    return null;
                } else if (numSchemas == 1) {
                    // Take the one and only schema. Returning null would probably also work fine.
                    return curSchema;
                } else {
                    throw new AdapterException("The data source is not expected to support schemas, but has "
                            + numSchemas + " schemas: " + Joiner.on(", ").join(allSchemas));
                }
            } else {
                throw new AdapterException(
                        "You specified a schema, however the data source does not support the concept of schemas.");
            }
        }
    }

    private static List<TableMetadata> findTables(final String catalog, final String schema,
            final List<String> tableFilter, final DatabaseMetaData dbMeta, final SqlDialect dialect,
            final JdbcAdapterProperties.ExceptionHandlingMode exceptionMode) throws SQLException {
        final List<TableMetadata> tables = new ArrayList<>();

        final String[] supportedTableTypes = { "TABLE", "VIEW", "SYSTEM TABLE" };

        final ResultSet resTables = dbMeta.getTables(catalog, schema, null, supportedTableTypes);
        final List<SqlDialect.MappedTable> tablesMapped = new ArrayList<>();
        // List<String> tableComments = new ArrayList<>();
        while (resTables.next()) {
            final SqlDialect.MappedTable mappedTable = dialect.mapTable(resTables);
            if (!mappedTable.isIgnored()) {
                tablesMapped.add(mappedTable);
                // tableComments.add(mappedTable.getTableComment());
            }
        }

        resTables.close();

        // Columns
        for (int i = 0; i < tablesMapped.size(); ++i) {
            final SqlDialect.MappedTable table = tablesMapped.get(i);
            System.out.println("Process columns for table: " + table);
            try {
                if (!tableFilter.isEmpty()) {
                    boolean isInFilter = false;
                    if (identifiersAreCaseInsensitive(dialect)) {
                        for (final String curTable : tableFilter) {
                            if (curTable.equalsIgnoreCase(table.getTableName())) {
                                isInFilter = true;
                            }
                        }
                    } else {
                        isInFilter = tableFilter.contains(table.getTableName());
                    }
                    if (!isInFilter) {
                        System.out.println("Skip table: " + table);
                        continue;
                    }
                }
                final List<ColumnMetadata> columns = readColumns(dbMeta, catalog, schema, table.getOriginalTableName(),
                        dialect, exceptionMode);
                if (columns != null) {
                    tables.add(new TableMetadata(table.getTableName(), "", columns, table.getTableComment()));
                }
            } catch (final Exception ex) {
                throw new RuntimeException("Exception for table " + table.getOriginalTableName(), ex);
            }
        }
        return tables;
    }

    private static boolean identifiersAreCaseInsensitive(final SqlDialect dialect) {
        return (dialect.getQuotedIdentifierHandling() == dialect.getUnquotedIdentifierHandling())
                && dialect.getQuotedIdentifierHandling() != SqlDialect.IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE;
    }

    private static List<ColumnMetadata> readColumns(final DatabaseMetaData dbMeta, final String catalog,
            final String schema, final String table, final SqlDialect dialect,
            final JdbcAdapterProperties.ExceptionHandlingMode exceptionMode) throws SQLException {
        final List<ColumnMetadata> columns = new ArrayList<>();
        try {
            final ResultSet cols = dbMeta.getColumns(catalog, schema, table, null);
            while (cols.next()) {
                columns.add(dialect.mapColumn(cols));
            }
            if (columns.isEmpty()) {
                System.out.println("Warning: Found a table without columns: " + table);
            }
            cols.close();
        } catch (final SQLException exception) {
            dialect.handleException(exception, exceptionMode);
            return null;
        }
        return columns;
    }
}
