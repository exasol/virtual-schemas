package com.exasol.adapter.jdbc;

import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

import com.exasol.*;
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

    private static final String CONNECTION_NAME_PROPERTY = "CONNECTION_NAME";
    private static final String IMPORT_FROM_EXA_PROPERTY = "IMPORT_FROM_EXA";
    private static final String IMPORT_FROM_ORA_PROPERTY = "IMPORT_FROM_ORA";
    private static final String EXA_CONNECTION_STRING_PROPERTY = "EXA_CONNECTION_STRING";
    private static final String ORA_CONNECTION_NAME_PROPERTY = "ORA_CONNECTION_NAME";
    private static final String TABLES_PROPERTY = "TABLE_FILTER";
    private static final String PROP_IS_LOCAL = "IS_LOCAL";
    private static final String PROP_EXCLUDED_CAPABILITIES = "EXCLUDED_CAPABILITIES";

    private static final Logger LOGGER = Logger.getLogger(JdbcAdapter.class.getName());
    private RemoteConnectionFactory factory = new RemoteConnectionFactory();

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
        return RequestDispatcher.adapterCall(metadata, rawRequest);
    }

    @Override
    public CreateVirtualSchemaResponse createVirtualSchema(final ExaMetadata exasolMetadata,
            final CreateVirtualSchemaRequest request) throws AdapterException {
        logCreateVirtualSchemaRequestReceived(request);
        final AdapterProperties properties = getPropertiesFromRequest(request);
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
            throws SQLException, PropertyValidationException {
        final List<String> tables = properties.getFilteredTables();
        try (final Connection connection = factory.createConnection(exasolMetadata, properties)) {
            final SqlDialect dialect = createDialect(connection, properties);
            dialect.validateProperties();
            if (tables.isEmpty()) {
                return dialect.readSchemaMetadata();
            } else {
                return dialect.readSchemaMetadata(tables);
            }
        }
    }

    private SchemaMetadata readMetadata(final AdapterProperties properties, final List<String> whiteListedRemoteTables,
            final ExaMetadata exasolMetadata) throws SQLException, PropertyValidationException {
        try (final Connection connection = factory.createConnection(exasolMetadata, properties)) {
            final SqlDialect dialect = createDialect(connection, properties);
            dialect.validateProperties();
            return dialect.readSchemaMetadata(whiteListedRemoteTables);
        }
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
        final Map<String, String> newSchemaMeta = getNewProperties(schemaMetadataInfo.getProperties(),
                changedProperties);
        final AdapterProperties properties = new AdapterProperties(newSchemaMeta);
        if (AdapterProperties.isRefreshingVirtualSchemaRequired(changedProperties)) {
            final List<String> tableFilter = getTableFilter(newSchemaMeta);
            final SchemaMetadata remoteMeta;
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

    private Map<String, String> getNewProperties(final Map<String, String> oldProperties,
            final Map<String, String> changedProperties) {
        final Map<String, String> newCompleteProperties = new HashMap<>(oldProperties);
        for (final Map.Entry<String, String> changedProperty : changedProperties.entrySet()) {
            if (changedProperty.getValue() == null) {
                newCompleteProperties.remove(changedProperty.getKey());
            } else {
                newCompleteProperties.put(changedProperty.getKey(), changedProperty.getValue());
            }
        }
        return newCompleteProperties;
    }

    private List<String> getTableFilter(final Map<String, String> properties) {
        final String tableNames = getProperty(properties, TABLES_PROPERTY);
        if (!tableNames.isEmpty()) {
            final List<String> tables = Arrays.asList(tableNames.split(","));
            for (int i = 0; i < tables.size(); ++i) {
                tables.set(i, tables.get(i).trim());
            }
            return tables;
        } else {
            return new ArrayList<>();
        }
    }

    @Override
    public GetCapabilitiesResponse getCapabilities(final ExaMetadata metadata, final GetCapabilitiesRequest request)
            throws AdapterException {
        LOGGER.fine(() -> "Received request to list the adapter's capabilites.");
        final AdapterProperties properties = getPropertiesFromRequest(request);
        final Connection connection = null;
        final SqlDialect dialect = createDialect(connection, properties);
        final Capabilities capabilities = dialect.getCapabilities();
        final Capabilities excludedCapabilities = parseExcludedCapabilities(
                getExcludedCapabilities(request.getSchemaMetadataInfo().getProperties()));
        capabilities.subtractCapabilities(excludedCapabilities);
        return GetCapabilitiesResponse //
                .builder()//
                .capabilities(capabilities)//
                .build();
    }

    private String getExcludedCapabilities(final Map<String, String> properties) {
        return getProperty(properties, PROP_EXCLUDED_CAPABILITIES);
    }

    private boolean isLocal(final Map<String, String> properties) {
        return getProperty(properties, PROP_IS_LOCAL).toUpperCase().equals("TRUE");
    }

    private Capabilities parseExcludedCapabilities(final String excludedCapabilitiesStr) {
        LOGGER.config(() -> "Excluded Capabilities: "
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
        try (final Connection connection = factory.createConnection(exasolMetadata, properties)) {
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
        final SqlGenerationContext context = new SqlGenerationContext(properties.getCatalogName(),
                properties.getSchemaName(), properties.isLocalSource());
        final SqlGenerationVisitor sqlGeneratorVisitor = dialect.getSqlGenerationVisitor(context);
        final String pushdownQuery = request.getSelect().accept(sqlGeneratorVisitor);
        LOGGER.finer(() -> "Push-down query:\n" + pushdownQuery);
        return pushdownQuery;
    }

    private ImportType getImportType(final SchemaMetadataInfo meta) {
        ImportType importType = ImportType.JDBC;
        if (isLocal(meta.getProperties())) {
            importType = ImportType.LOCAL;
        } else if (isImportFromExa(meta.getProperties())) {
            importType = ImportType.EXA;
        } else if (isImportFromOra(meta.getProperties())) {
            importType = ImportType.ORA;
        }
        return importType;
    }

    private boolean isImportFromExa(final Map<String, String> properties) {
        return getProperty(properties, IMPORT_FROM_EXA_PROPERTY).toUpperCase().equals("TRUE");
    }

    private boolean isImportFromOra(final Map<String, String> properties) {
        return getProperty(properties, IMPORT_FROM_ORA_PROPERTY).toUpperCase().equals("TRUE");
    }

    private ConnectionInformation getConnectionInformation(final ExaMetadata exaMeta, final SchemaMetadataInfo meta) {
        final String credentials = getCredentialsForPushdownQuery(exaMeta, meta);
        final String exaConnectionString = getExaConnectionString(meta.getProperties());
        final String oraConnectionName = getOraConnectionName(meta.getProperties());
        return new ConnectionInformation(credentials, exaConnectionString, oraConnectionName);
    }

    private String getExaConnectionString(final Map<String, String> properties) {
        return getProperty(properties, EXA_CONNECTION_STRING_PROPERTY);
    }

    private String getOraConnectionName(final Map<String, String> properties) {
        return getProperty(properties, ORA_CONNECTION_NAME_PROPERTY);
    }

    protected String getCredentialsForPushdownQuery(final ExaMetadata exaMeta, final SchemaMetadataInfo meta) {
        String credentials = "";
        if (isImportFromExa(meta.getProperties())) {
            credentials = getCredentialsForEXAImport(exaMeta, meta);
        } else if (isImportFromOra(meta.getProperties())) {
            credentials = getCredentialsForORAImport(exaMeta, meta);
        } else {
            credentials = getCredentialsForJDBCImport(exaMeta, meta);
        }
        return credentials;
    }

    private String getCredentialsForJDBCImport(final ExaMetadata exaMeta, final SchemaMetadataInfo meta) {
        String credentials = "";
        if (isUserSpecifiedConnection(meta.getProperties())) {
            credentials = getConnectionName(meta.getProperties());
        } else {
            credentials = getUserAndPasswordForImport(exaMeta, meta);
            credentials = "'" + new AdapterProperties(meta.getProperties()).getConnectionString() + "' " + credentials;
        }
        return credentials;
    }

    private boolean isUserSpecifiedConnection(final Map<String, String> properties) {
        final String connName = getProperty(properties, CONNECTION_NAME_PROPERTY);
        return ((connName != null) && !connName.isEmpty());
    }

    private String getConnectionName(final Map<String, String> properties) {
        final String connName = getProperty(properties, CONNECTION_NAME_PROPERTY);
        assert ((connName != null) && !connName.isEmpty());
        return connName;
    }

    private static String getProperty(final Map<String, String> properties, final String name) {
        return properties.getOrDefault(name, "");
    }

    private String getCredentialsForORAImport(final ExaMetadata exaMeta, final SchemaMetadataInfo meta) {
        String credentials = "";
        if (!isUserSpecifiedConnection(meta.getProperties())) {
            credentials = getUserAndPasswordForImport(exaMeta, meta);
        }
        return credentials;
    }

    private String getCredentialsForEXAImport(final ExaMetadata exaMeta, final SchemaMetadataInfo meta) {
        return getUserAndPasswordForImport(exaMeta, meta);
    }

    private String getUserAndPasswordForImport(final ExaMetadata exasolMetadata,
            final SchemaMetadataInfo schemaMetadataInfo) {
        String credentials = "";
        String username = getUserName(exasolMetadata, new AdapterProperties(schemaMetadataInfo.getProperties()));
        String password = getPassword(exasolMetadata, new AdapterProperties(schemaMetadataInfo.getProperties()));
        if ((username != null) || (password != null)) {
            credentials = "USER '" + username + "' IDENTIFIED BY '" + password + "'";
        }
        return credentials;
    }

    private String getUserName(final ExaMetadata exasolMetadata, AdapterProperties adapterProperties) {
        final String connectionName = adapterProperties.getConnectionName();
        if ((connectionName != null) && !connectionName.isEmpty()) {
            try {
                return exasolMetadata.getConnection(connectionName).getUser();
            } catch (final ExaConnectionAccessException exception) {
                throw new RuntimeException(
                        "Could not access the connection information of connection \"" + connectionName + "\"",
                        exception);
            }
        } else {
            return adapterProperties.getUsername();
        }
    }

    private String getPassword(final ExaMetadata exasolMetadata, AdapterProperties adapterProperties) {
        final String connectionName = adapterProperties.getConnectionName();
        if ((connectionName != null) && !connectionName.isEmpty()) {
            try {
                return exasolMetadata.getConnection(connectionName).getPassword();
            } catch (final ExaConnectionAccessException exception) {
                throw new RuntimeException(
                        "Could not access the connection information of connection \"" + connectionName + "\"",
                        exception);
            }
        } else {
            return adapterProperties.getPassword();
        }
    }
}