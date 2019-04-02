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
import com.exasol.adapter.metadata.*;
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
        registerAdapterForSqlDialect(adapter, "MYSQL"); // FIXME: replace this hard-coded registration
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
    public CreateVirtualSchemaResponse createVirtualSchema(final ExaMetadata metadata,
            final CreateVirtualSchemaRequest request) throws AdapterException {
        final SchemaMetadataInfo schemaMetadataInfo = request.getSchemaMetadataInfo();
        JdbcAdapterProperties.checkPropertyConsistency(schemaMetadataInfo.getProperties());
        try {
            final SchemaMetadata remoteMeta = readMetadata(schemaMetadataInfo, metadata);
            return CreateVirtualSchemaResponse.builder().schemaMetadata(remoteMeta).build();
        } catch (final SQLException exception) {
            throw new AdapterException("Unable create Virtual Schema \"" + schemaMetadataInfo.getSchemaName() + "\".",
                    exception);
        }
    }

    private SchemaMetadata readMetadata(final SchemaMetadataInfo schemaMeta, final ExaMetadata meta)
            throws SQLException, AdapterException {
        final List<String> tables = JdbcAdapterProperties.getTableFilter(schemaMeta.getProperties());
        return readMetadata(schemaMeta, tables, meta);
    }

    private SchemaMetadata readMetadata(final SchemaMetadataInfo meta, final List<String> tables,
            final ExaMetadata exaMeta) throws SQLException, AdapterException {
        // Connect via JDBC and read metadata
        final ExaConnectionInformation connection = JdbcAdapterProperties.getConnectionInformation(meta.getProperties(),
                exaMeta);
        final String catalog = JdbcAdapterProperties.getCatalog(meta.getProperties());
        final String schema = JdbcAdapterProperties.getSchema(meta.getProperties());
        return JdbcMetadataReader.readRemoteMetadata(connection.getAddress(), connection.getUser(),
                connection.getPassword(), catalog, schema, tables,
                JdbcAdapterProperties.getSqlDialectName(meta.getProperties()),
                JdbcAdapterProperties.getExceptionHandlingMode(meta.getProperties()),
                JdbcAdapterProperties.getIgnoreErrorList(meta.getProperties()),
                getPostgreSQLIdentifierMapping(meta.getProperties()),
                JdbcAdapterProperties.getOracleNumberTargetType(meta.getProperties()));
    }

    @Override
    public DropVirtualSchemaResponse dropVirtualSchema(final ExaMetadata metadata,
            final DropVirtualSchemaRequest request) {
        return DropVirtualSchemaResponse.builder().build();
    }

    @Override
    public RefreshResponse refresh(final ExaMetadata metadata, final RefreshRequest request) throws AdapterException {
        final SchemaMetadataInfo schemaMetadataInfo = request.getSchemaMetadataInfo();
        JdbcAdapterProperties.checkPropertyConsistency(schemaMetadataInfo.getProperties());
        final SchemaMetadata remoteMeta;
        try {
            if (request.refreshesOnlySelectedTables()) {
                final List<String> tables = request.getTables();
                remoteMeta = readMetadata(request.getSchemaMetadataInfo(), tables, metadata);
            } else {
                remoteMeta = readMetadata(request.getSchemaMetadataInfo(), metadata);
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
        JdbcAdapterProperties.checkPropertyConsistency(newSchemaMeta);
        if (JdbcAdapterProperties.isRefreshNeeded(changedProperties)) {
            final ExaConnectionInformation connection = JdbcAdapterProperties.getConnectionInformation(newSchemaMeta,
                    metadata);
            final List<String> tableFilter = JdbcAdapterProperties.getTableFilter(newSchemaMeta);
            final SchemaMetadata remoteMeta;
            try {
                remoteMeta = JdbcMetadataReader.readRemoteMetadata(connection.getAddress(), connection.getUser(),
                        connection.getPassword(), JdbcAdapterProperties.getCatalog(newSchemaMeta),
                        JdbcAdapterProperties.getSchema(newSchemaMeta), tableFilter,
                        JdbcAdapterProperties.getSqlDialectName(newSchemaMeta),
                        JdbcAdapterProperties.getExceptionHandlingMode(newSchemaMeta),
                        JdbcAdapterProperties.getIgnoreErrorList(newSchemaMeta),
                        getPostgreSQLIdentifierMapping(newSchemaMeta),
                        JdbcAdapterProperties.getOracleNumberTargetType(newSchemaMeta));
            } catch (final SQLException exception) {
                throw new AdapterException(
                        "Unable set properties of Virtual Schema \"" + schemaMetadataInfo.getSchemaName() + "\".",
                        exception);
            }
            return SetPropertiesResponse.builder().schemaMetadata(remoteMeta).build();
        }
        return SetPropertiesResponse.builder().schemaMetadata(null).build();
    }

    @Override
    public GetCapabilitiesResponse getCapabilities(final ExaMetadata metadata, final GetCapabilitiesRequest request)
            throws AdapterException {
        final SqlDialectContext dialectContext = new SqlDialectContext(SchemaAdapterNotes.deserialize(
                request.getSchemaMetadataInfo().getAdapterNotes(), request.getSchemaMetadataInfo().getSchemaName()));
        final SqlDialect dialect = JdbcAdapterProperties.getSqlDialect(request.getSchemaMetadataInfo().getProperties(),
                dialectContext);
        final Capabilities capabilities = dialect.getCapabilities();
        final Capabilities excludedCapabilities = parseExcludedCapabilities(
                JdbcAdapterProperties.getExcludedCapabilities(request.getSchemaMetadataInfo().getProperties()));
        capabilities.subtractCapabilities(excludedCapabilities);
        return GetCapabilitiesResponse.builder().capabilities(capabilities).build();
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
    public PushDownResponse pushdown(final ExaMetadata metadata, final PushDownRequest request)
            throws AdapterException {
        final SchemaMetadataInfo schemaMetadataInfo = request.getSchemaMetadataInfo();
        final PostgreSQLIdentifierMapping postgreSQLIdentifierMapping = getPostgreSQLIdentifierMapping(
                schemaMetadataInfo.getProperties());
        final ImportType importType = getImportType(schemaMetadataInfo);

        final SqlDialectContext dialectContext = new SqlDialectContext(
                SchemaAdapterNotes.deserialize(request.getSchemaMetadataInfo().getAdapterNotes(),
                        request.getSchemaMetadataInfo().getSchemaName()),
                postgreSQLIdentifierMapping, getImportType(schemaMetadataInfo),
                JdbcAdapterProperties.getOracleNumberTargetType(schemaMetadataInfo.getProperties()));
        final SqlDialect dialect = JdbcAdapterProperties.getSqlDialect(request.getSchemaMetadataInfo().getProperties(),
                dialectContext);
        final boolean hasMoreThanOneTable = request.getInvolvedTablesMetadata().size() > 1;
        final SqlGenerationContext context = new SqlGenerationContext(
                JdbcAdapterProperties.getCatalog(schemaMetadataInfo.getProperties()),
                JdbcAdapterProperties.getSchema(schemaMetadataInfo.getProperties()),
                JdbcAdapterProperties.isLocal(schemaMetadataInfo.getProperties()), hasMoreThanOneTable);
        final SqlGenerationVisitor sqlGeneratorVisitor = dialect.getSqlGenerationVisitor(context);
        final String pushdownQuery = request.getSelect().accept(sqlGeneratorVisitor);

        final ConnectionInformation connectionInformation = getConnectionInformation(metadata, schemaMetadataInfo);
        String columnDescription = "";
        if (importType == ImportType.JDBC) {
            columnDescription = createColumnDescription(metadata, schemaMetadataInfo, pushdownQuery, dialect);
        }
        final String sql = dialect.generatePushdownSql(connectionInformation, columnDescription, pushdownQuery);
        return PushDownResponse.builder().pushDownSql(sql).build();
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
                    .getConnectionInformation(meta.getProperties(), exaMeta);
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
        final ExaConnectionInformation connection = JdbcAdapterProperties.getConnectionInformation(meta.getProperties(),
                exaMeta);
        if ((connection.getUser() != null) || (connection.getPassword() != null)) {
            credentials = "USER '" + connection.getUser() + "' IDENTIFIED BY '" + connection.getPassword() + "'";
        }
        return credentials;
    }

    private String createColumnDescription(final ExaMetadata exaMeta, final SchemaMetadataInfo meta,
            final String pushdownQuery, final SqlDialect dialect) {
        final ExaConnectionInformation connectionInformation = JdbcAdapterProperties
                .getConnectionInformation(meta.getProperties(), exaMeta);
        try {
            final Connection connection = establishConnection(connectionInformation);
            LOGGER.fine(() -> "createColumnDescription: " + pushdownQuery);
            DataType[] internalTypes = null;
            try (final PreparedStatement ps = connection.prepareStatement(pushdownQuery)) {
                ResultSetMetaData metadata = ps.getMetaData();
                if (metadata == null) {
                    ps.execute();
                    metadata = ps.getMetaData();
                    if (metadata == null) {
                        throw new SQLException(
                                "Unable to read source metadata trying to create description for " + "source columns.");
                    }
                }
                internalTypes = new DataType[metadata.getColumnCount()];
                for (int col = 1; col <= metadata.getColumnCount(); ++col) {
                    final JdbcTypeDescription description = getJdbcTypeDescription(metadata, col);
                    internalTypes[col - 1] = dialect.mapJdbcType(description);
                }
            }
            return buildColumnDescriptionFrom(internalTypes);
        } catch (final SQLException e) {
            throw new RetrieveMetadataException("Cannot resolve column types. " + e.getMessage(), e);
        }
    }

    protected static JdbcTypeDescription getJdbcTypeDescription(final ResultSetMetaData metadata, final int col)
            throws SQLException {
        final int jdbcType = metadata.getColumnType(col);
        final int jdbcPrecisions = metadata.getPrecision(col);
        final int jdbcScales = metadata.getScale(col);
        return new JdbcTypeDescription(jdbcType, jdbcScales, jdbcPrecisions, 0, metadata.getColumnTypeName(col));
    }

    protected static String buildColumnDescriptionFrom(final DataType[] internalTypes) {
        final StringBuilder columnDescription = new StringBuilder();
        columnDescription.append('(');
        for (int i = 0; i < internalTypes.length; i++) {
            columnDescription.append("c");
            columnDescription.append(i);
            columnDescription.append(" ");
            columnDescription.append(internalTypes[i].toString());
            if (i < (internalTypes.length - 1)) {
                columnDescription.append(",");
            }
        }
        columnDescription.append(')');
        return columnDescription.toString();
    }

    private static Connection establishConnection(final ExaConnectionInformation connection) throws SQLException {
        final String connectionString = connection.getAddress();
        final String user = connection.getUser();
        final String password = connection.getPassword();
        LOGGER.fine(() -> "Connection parameters: " + connectionString);

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

    private PostgreSQLIdentifierMapping getPostgreSQLIdentifierMapping(final Map<String, String> properties) {
        final String postgreSQLIdentifierMapping = JdbcAdapterProperties.getPostgreSQLIdentifierMapping(properties);
        return PostgreSQLIdentifierMapping.valueOf(postgreSQLIdentifierMapping);
    }
}