package com.exasol.adapter.jdbc;

import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.exasol.ExaConnectionInformation;
import com.exasol.ExaMetadata;
import com.exasol.adapter.*;
import com.exasol.adapter.capabilities.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.metadata.SchemaMetadataInfo;
import com.exasol.adapter.request.*;
import com.exasol.adapter.response.*;

public class JdbcAdapter implements VirtualSchemaAdapter {
    private static final String SCALAR_FUNCTION_PREFIX = "FN_";
    private static final String PREDICATE_PREFIX = "FN_PRED_";
    private static final String AGGREGATE_FUNCTION_PREFIX = "FN_AGG_";
    private static final String LITERAL_PREFIX = "LITERAL_";
    private static final Logger LOGGER = Logger.getLogger(JdbcAdapter.class.getName());

    /**
     * This method gets called by the database during interactions with the virtual schema.
     *
     * @param metadata   Metadata object
     * @param rawRequest JSON request, as defined in the Adapter Script API
     * @return JSON response, as defined in the Adapter Script API
     * @throws AdapterException in case the request is not recognized
     * @deprecated As of Virtual Schema version 1.8.0 you should use
     *             {@link com.exasol.adapter.RequestDispatcher#adapterCall(ExaMetadata, String)} as entry point instead.
     */
    @Deprecated
    public static String adapterCall(final ExaMetadata metadata, final String rawRequest) throws AdapterException {
        final VirtualSchemaAdapter adapter = new JdbcAdapter();
        registerAdapterForSqlDialect(adapter, "DB2"); // FIXME: replace this hard-coded registration
        registerAdapterForSqlDialect(adapter, "EXASOL"); // FIXME: replace this hard-coded registration
        registerAdapterForSqlDialect(adapter, "GENERIC"); // FIXME: replace this hard-coded registration
        registerAdapterForSqlDialect(adapter, "IMPALA"); // FIXME: replace this hard-coded registration
        registerAdapterForSqlDialect(adapter, "ORACLE"); // FIXME: replace this hard-coded registration
        registerAdapterForSqlDialect(adapter, "POSTGRESQL"); // FIXME: replace this hard-coded registration
        registerAdapterForSqlDialect(adapter, "REDSHIFT"); // FIXME: replace this hard-coded registration
        registerAdapterForSqlDialect(adapter, "SQLSERVER"); // FIXME: replace this hard-coded registration
        registerAdapterForSqlDialect(adapter, "SYBASE"); // FIXME: replace this hard-coded registration
        registerAdapterForSqlDialect(adapter, "TERADATA"); // FIXME: replace this hard-coded registration
        return RequestDispatcher.adapterCall(metadata, rawRequest);
    }

    private static void registerAdapterForSqlDialect(final VirtualSchemaAdapter adapter, final String dialectName) {
        AdapterRegistry.getInstance().registerAdapter(dialectName, adapter);
    }

    @Override
    public CreateVirtualSchemaResponse createVirtualSchema(final ExaMetadata exasolMetadata,
            final CreateVirtualSchemaRequest request) throws AdapterException {
        logCreateVirtualSchemaRequestReceived(request);
        final SchemaMetadataInfo schemaMetadataInfo = request.getSchemaMetadataInfo();
        final AdapterProperties properties = getPropertiesFromRequest(request);
        JdbcAdapterProperties.checkPropertyConsistency(schemaMetadataInfo.getProperties());
        try {
            final SchemaMetadata remoteMeta = readMetadata(properties, exasolMetadata);
            return CreateVirtualSchemaResponse.builder().schemaMetadata(remoteMeta).build();
        } catch (final SQLException exception) {
            throw new AdapterException("Unable create Virtual Schema \"" + request.getVirtualSchemaName() + "\".",
                    exception);
        }
    }

    protected void logCreateVirtualSchemaRequestReceived(final CreateVirtualSchemaRequest request) {
        LOGGER.fine(() -> "Received request to create Virutal Schema \"" + request.getVirtualSchemaName() + "\".");
    }

    private AdapterProperties getPropertiesFromRequest(final AdapterRequest request) {
        return new AdapterProperties(request.getSchemaMetadataInfo().getProperties());
    }

    private SchemaMetadata readMetadata(final AdapterProperties properties, final ExaMetadata exasolMetadata)
            throws SQLException {
        final List<String> tables = properties.getFilteredTables();
        try (final Connection connection = getConnection(exasolMetadata, properties)) {
            final SqlDialect dialect = createDialect(connection, properties);
            if (tables.isEmpty()) {
                return dialect.readSchemaMetadata();
            } else {
                return dialect.readSchemaMetadata(tables);
            }
        }
    }

    private SchemaMetadata readMetadata(final AdapterProperties properties, final List<String> whiteListedRemoteTables,
            final ExaMetadata exasolMetadata) throws SQLException {
        try (final Connection connection = getConnection(exasolMetadata, properties)) {
            final SqlDialect dialect = createDialect(connection, properties);
            return dialect.readSchemaMetadata(whiteListedRemoteTables);
        }
    }

    private Connection getConnection(final ExaMetadata exasolMetadata, final AdapterProperties properties)
            throws SQLException {
        final ExaConnectionInformation connectionInformation = JdbcAdapterProperties
                .getConnectionInformation(properties, exasolMetadata);
        final String address = connectionInformation.getAddress();
        final String user = connectionInformation.getUser();
        final String password = connectionInformation.getPassword();
        logConnectionAttempt(address, user);
        final Connection connection = DriverManager.getConnection(address, user, password);
        logRemoteDatabaseDetails(connection);
        return connection;
    }

    protected void logConnectionAttempt(final String address, final String user) {
        LOGGER.fine(() -> "Connecting to \"" + address + "\" as user \"" + user + "\"");
    }

    protected void logRemoteDatabaseDetails(final Connection connection) throws SQLException {
        final String databaseProductName = connection.getMetaData().getDatabaseProductName();
        final String databaseProductVersion = connection.getMetaData().getDatabaseProductVersion();
        LOGGER.info(() -> "Connected to " + databaseProductName + " " + databaseProductVersion);
    }

    private SqlDialect createDialect(final Connection connection, final AdapterProperties properties) {
        final SqlDialectFactory factory = new SqlDialectFactory(connection, SqlDialectRegistry.getInstance(),
                properties);
        return factory.createSqlDialect(properties.getSqlDialect());
    }

    @Override
    public DropVirtualSchemaResponse dropVirtualSchema(final ExaMetadata metadata,
            final DropVirtualSchemaRequest request) {
        logDropVirtualSchemaRequestReceived(request);
        return DropVirtualSchemaResponse.builder().build();
    }

    protected void logDropVirtualSchemaRequestReceived(final DropVirtualSchemaRequest request) {
        LOGGER.fine(() -> "Received request to drop Virutal Schema \"" + request.getVirtualSchemaName() + "\".");
    }

    @Override
    public RefreshResponse refresh(final ExaMetadata metadata, final RefreshRequest request) throws AdapterException {
        final SchemaMetadataInfo schemaMetadataInfo = request.getSchemaMetadataInfo();
        final AdapterProperties properties = getPropertiesFromRequest(request);
        JdbcAdapterProperties.checkPropertyConsistency(schemaMetadataInfo.getProperties());
        final SchemaMetadata remoteMeta;
        try {
            if (request.refreshesOnlySelectedTables()) {
                final List<String> tables = request.getTables();
                remoteMeta = readMetadata(properties, tables, metadata);
            } else {
                remoteMeta = readMetadata(properties, metadata);
            }
            return RefreshResponse.builder().schemaMetadata(remoteMeta).build();
        } catch (final SQLException exception) {
            throw new AdapterException(
                    "Unable refresh metadata of Virtual Schema \"" + schemaMetadataInfo.getSchemaName() + "\".",
                    exception);
        }
    }

    @Override
    public SetPropertiesResponse setProperties(final ExaMetadata metadata, final SetPropertiesRequest request)
            throws AdapterException {
        final Map<String, String> changedProperties = request.getProperties();
        final SchemaMetadataInfo schemaMetadataInfo = request.getSchemaMetadataInfo();
        final Map<String, String> newSchemaMeta = JdbcAdapterProperties
                .getNewProperties(schemaMetadataInfo.getProperties(), changedProperties);
        final AdapterProperties properties = new AdapterProperties(newSchemaMeta);
        JdbcAdapterProperties.checkPropertyConsistency(newSchemaMeta);
        if (AdapterProperties.isRefreshingVirtualSchemaRequired(changedProperties)) {
            final List<String> tableFilter = JdbcAdapterProperties.getTableFilter(newSchemaMeta);
            SchemaMetadata remoteMeta;
            try {
                remoteMeta = readMetadata(properties, tableFilter, metadata);
            } catch (final SQLException exception) {
                throw new AdapterException(
                        "Unable to set new properties for the virtual schema \"" + schemaMetadataInfo.getSchemaName()
                                + ("\", because metadata of the remote source could not be read."),
                        exception);
            }
            return SetPropertiesResponse.builder().schemaMetadata(remoteMeta).build();
        }
        return SetPropertiesResponse.builder().schemaMetadata(null).build();
    }

    @Override
    public GetCapabilitiesResponse getCapabilities(final ExaMetadata metadata, final GetCapabilitiesRequest request)
            throws AdapterException {
        final AdapterProperties properties = getPropertiesFromRequest(request);
        final Connection connection = null;
        final SqlDialect dialect = createDialect(connection, properties);
        final Capabilities capabilities = dialect.getCapabilities();
        final Capabilities excludedCapabilities = parseExcludedCapabilities(
                JdbcAdapterProperties.getExcludedCapabilities(request.getSchemaMetadataInfo().getProperties()));
        capabilities.subtractCapabilities(excludedCapabilities);
        return GetCapabilitiesResponse //
                .builder()//
                .capabilities(capabilities)//
                .build();
    }

    private Capabilities parseExcludedCapabilities(final String excludedCapabilitiesStr) {
        LOGGER.info(() -> "Excluded Capabilities: "
                + (excludedCapabilitiesStr.isEmpty() ? "none" : excludedCapabilitiesStr));
        final Capabilities.Builder builder = Capabilities.builder();
        for (String capability : excludedCapabilitiesStr.split(",")) {
            capability = capability.trim();
            if (capability.isEmpty()) {
                continue;
            }
            if (capability.startsWith(LITERAL_PREFIX)) {
                final String literalCapabilities = capability.replaceFirst(LITERAL_PREFIX, "");
                builder.addLiteral(LiteralCapability.valueOf(literalCapabilities));
            } else if (capability.startsWith(AGGREGATE_FUNCTION_PREFIX)) {
                // Aggregate functions must be checked before scalar functions
                final String aggregateFunctionCap = capability.replaceFirst(AGGREGATE_FUNCTION_PREFIX, "");
                builder.addAggregateFunction(AggregateFunctionCapability.valueOf(aggregateFunctionCap));
            } else if (capability.startsWith(SCALAR_FUNCTION_PREFIX)) {
                final String scalarFunctionCapabilities = capability.replaceFirst(SCALAR_FUNCTION_PREFIX, "");
                builder.addScalarFunction(ScalarFunctionCapability.valueOf(scalarFunctionCapabilities));
            } else if (capability.startsWith(PREDICATE_PREFIX)) {
                final String predicateCapabilities = capability.replaceFirst(SCALAR_FUNCTION_PREFIX, "");
                builder.addScalarFunction(ScalarFunctionCapability.valueOf(predicateCapabilities));
            } else {
                builder.addMain(MainCapability.valueOf(capability));
            }
        }
        return builder.build();
    }

    @Override
    public PushDownResponse pushdown(final ExaMetadata exasolMetadata, final PushDownRequest request)
            throws AdapterException {
        final SchemaMetadataInfo schemaMetadataInfo = request.getSchemaMetadataInfo();
        final AdapterProperties properties = getPropertiesFromRequest(request);
        try (final Connection connection = getConnection(exasolMetadata, properties)) {
            final SqlDialect dialect = createDialect(connection, properties);
            final String pushdownQuery = createPushdownQuery(request, properties, dialect);
            final ConnectionInformation connectionInformation = getConnectionInformation(exasolMetadata,
                    schemaMetadataInfo);
            final String columnDescription = createImportColumnsDescription(schemaMetadataInfo, dialect, pushdownQuery);
            final String importFromPushdownQuery = dialect.generatePushdownSql(connectionInformation, columnDescription,
                    pushdownQuery);
            LOGGER.finer(() -> "Import from push-down query:\n" + importFromPushdownQuery);
            return PushDownResponse.builder().pushDownSql(importFromPushdownQuery).build();
        } catch (final SQLException exception) {
            throw new AdapterException("Unable to execute push-down request.", exception);
        }
    }

    protected String createImportColumnsDescription(final SchemaMetadataInfo schemaMetadataInfo,
            final SqlDialect dialect, final String pushdownQuery) throws SQLException {
        final ImportType importType = getImportType(schemaMetadataInfo);
        final String columnsDescription = (importType == ImportType.JDBC)
                ? dialect.describeQueryResultColumns(pushdownQuery)
                : "";
        LOGGER.finer(() -> "Import columns " + columnsDescription);
        return columnsDescription;
    }

    protected String createPushdownQuery(final PushDownRequest request, final AdapterProperties properties,
            final SqlDialect dialect) throws AdapterException {
        final boolean hasMoreThanOneTable = request.getInvolvedTablesMetadata().size() > 1;
        final SqlGenerationContext context = new SqlGenerationContext(properties.getCatalogName(),
                properties.getSchemaName(), properties.isLocalSource(), hasMoreThanOneTable);
        final SqlGenerationVisitor sqlGeneratorVisitor = dialect.getSqlGenerationVisitor(context);
        final String pushdownQuery = request.getSelect().accept(sqlGeneratorVisitor);
        LOGGER.finer(() -> "Push-down query:\n" + pushdownQuery);
        return pushdownQuery;
    }

    private ImportType getImportType(final SchemaMetadataInfo meta) {
        ImportType importType = ImportType.JDBC;
        if (JdbcAdapterProperties.isLocal(meta.getProperties())) {
            importType = ImportType.LOCAL;
        } else if (JdbcAdapterProperties.isImportFromExa(meta.getProperties())) {
            importType = ImportType.EXA;
        } else if (JdbcAdapterProperties.isImportFromOra(meta.getProperties())) {
            importType = ImportType.ORA;
        }
        return importType;
    }

    private ConnectionInformation getConnectionInformation(final ExaMetadata exaMeta, final SchemaMetadataInfo meta) {
        final String credentials = getCredentialsForPushdownQuery(exaMeta, meta);
        final String exaConnectionString = JdbcAdapterProperties.getExaConnectionString(meta.getProperties());
        final String oraConnectionName = JdbcAdapterProperties.getOraConnectionName(meta.getProperties());
        return new ConnectionInformation(credentials, exaConnectionString, oraConnectionName);
    }

    protected String getCredentialsForPushdownQuery(final ExaMetadata exaMeta, final SchemaMetadataInfo meta) {
        String credentials = "";
        if (JdbcAdapterProperties.isImportFromExa(meta.getProperties())) {
            credentials = getCredentialsForEXAImport(exaMeta, meta);
        } else if (JdbcAdapterProperties.isImportFromOra(meta.getProperties())) {
            credentials = getCredentialsForORAImport(exaMeta, meta);
        } else {
            credentials = getCredentialsForJDBCImport(exaMeta, meta);
        }
        return credentials;
    }

    private String getCredentialsForJDBCImport(final ExaMetadata exaMeta, final SchemaMetadataInfo meta) {
        String credentials = "";
        if (JdbcAdapterProperties.isUserSpecifiedConnection(meta.getProperties())) {
            credentials = JdbcAdapterProperties.getConnectionName(meta.getProperties());
        } else {
            credentials = getUserAndPasswordForImport(exaMeta, meta);
            final ExaConnectionInformation connection = JdbcAdapterProperties
                    .getConnectionInformation(new AdapterProperties(meta.getProperties()), exaMeta);
            credentials = "'" + connection.getAddress() + "' " + credentials;
        }
        return credentials;
    }

    private String getCredentialsForORAImport(final ExaMetadata exaMeta, final SchemaMetadataInfo meta) {
        String credentials = "";
        if (!JdbcAdapterProperties.isUserSpecifiedConnection(meta.getProperties())) {
            credentials = getUserAndPasswordForImport(exaMeta, meta);
        }
        return credentials;
    }

    private String getCredentialsForEXAImport(final ExaMetadata exaMeta, final SchemaMetadataInfo meta) {
        return getUserAndPasswordForImport(exaMeta, meta);
    }

    private String getUserAndPasswordForImport(final ExaMetadata exaMeta, final SchemaMetadataInfo meta) {
        String credentials = "";
        final ExaConnectionInformation connection = JdbcAdapterProperties
                .getConnectionInformation(new AdapterProperties(meta.getProperties()), exaMeta);
        if ((connection.getUser() != null) || (connection.getPassword() != null)) {
            credentials = "USER '" + connection.getUser() + "' IDENTIFIED BY '" + connection.getPassword() + "'";
        }
        return credentials;
    }
}