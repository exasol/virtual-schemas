package com.exasol.adapter.jdbc;

import java.io.OutputStream;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.logging.*;

import com.exasol.ExaConnectionInformation;
import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.capabilities.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.json.RequestJsonParser;
import com.exasol.adapter.json.ResponseJsonSerializer;
import com.exasol.adapter.metadata.*;
import com.exasol.adapter.request.*;
import com.exasol.logging.CompactFormatter;
import com.exasol.utils.JsonHelper;
import com.exasol.utils.UdfUtils;

public class JdbcAdapter {
    public static final int MAX_STRING_CHAR_LENGTH = 2000000;
    private static Logger logger = null;

    /**
     * This method gets called by the database during interactions with the virtual
     * schema.
     *
     * @param meta Metadata object
     * @param input JSON request, as defined in the Adapter Script API
     * @return JSON response, as defined in the Adapter Script API
     */
    public static String adapterCall(final ExaMetadata meta, final String input) throws Exception {
        String result = "";
        try {
            final AdapterRequest request = new RequestJsonParser().parseRequest(input);
            final SchemaMetadataInfo schemaMetadata = request.getSchemaMetadataInfo();
            configureLogOutput(schemaMetadata);
            logger.fine(() -> "Adapter request:\n" + input);

            switch (request.getType()) {
            case CREATE_VIRTUAL_SCHEMA:
                result = handleCreateVirtualSchema((CreateVirtualSchemaRequest) request, meta);
                break;
            case DROP_VIRTUAL_SCHEMA:
                result = handleDropVirtualSchema((DropVirtualSchemaRequest) request);
                break;
            case REFRESH:
                result = handleRefresh((RefreshRequest) request, meta);
                break;
            case SET_PROPERTIES:
                result = handleSetProperty((SetPropertiesRequest) request, meta);
                break;
            case GET_CAPABILITIES:
                result = handleGetCapabilities((GetCapabilitiesRequest) request);
                break;
            case PUSHDOWN:
                result = handlePushdownRequest((PushdownRequest) request, meta);
                break;
            default:
                throw new RuntimeException("Request Type not supported: " + request.getType());
            }
            assert (result.isEmpty());
            logger.fine("Response:\n" + JsonHelper.prettyJson(JsonHelper.getJsonObject(result)));
            return result;
        } catch (final AdapterException e) {
            throw e;
        } catch (final Exception e) {
            String stacktrace = UdfUtils.traceToString(e);
            throw new Exception("Unexpected error in adapter: " + e.getMessage() + "\nFor following request: " + input + "\nResponse: " + result + "\nAdapter stack trace: " + stacktrace,
                    e);
        }
    }

    private static void configureLogOutput(final SchemaMetadataInfo schemaMetadata)
            throws AdapterException, InvalidPropertyException {
        final OutputStream out = tryAttachToOutputService(schemaMetadata);
        if (out == null) {
            // Fall back to regular STDOUT in case the socket output stream is not
            // available. In most cases (except unit test scenarios) this will mean that
            // logs will not be available.
            configureLogger(System.out, schemaMetadata.getProperties());
        } else {
            configureLogger(out, schemaMetadata.getProperties());
        }

    }

    private static synchronized void configureLogger(final OutputStream out, final Map<String, String> properties)
            throws InvalidPropertyException {
        if (logger == null) {
            final Level logLevel = determineLogLevel(properties);
            final Formatter formatter = new CompactFormatter();
            final StreamHandler handler = new StreamHandler(out, formatter);
            handler.setFormatter(formatter);
            handler.setLevel(logLevel);
            final Logger baseLogger = Logger.getLogger("com.exasol");
            baseLogger.setLevel(logLevel);
            baseLogger.addHandler(handler);
            logger = Logger.getLogger(JdbcAdapter.class.getName());
            logger.info(() -> "Attached to output service with log level " + logLevel + ".");
        }
    }

    private static Level determineLogLevel(final Map<String, String> properties) throws InvalidPropertyException {
        return (JdbcAdapterProperties.getLogLevel(properties) == null) //
                ? Level.INFO //
                : JdbcAdapterProperties.getLogLevel(properties);
    }

    private static String handleCreateVirtualSchema(final CreateVirtualSchemaRequest request, final ExaMetadata meta)
            throws SQLException, AdapterException {
        final SchemaMetadataInfo schemaMetadata = request.getSchemaMetadataInfo();
        final Map<String, String> properties = schemaMetadata.getProperties();
        configureLogOutput(schemaMetadata);
        JdbcAdapterProperties.checkPropertyConsistency(properties);
        final SchemaMetadata remoteMeta = readMetadata(schemaMetadata, meta);
        return ResponseJsonSerializer.makeCreateVirtualSchemaResponse(remoteMeta);
    }

    private static SchemaMetadata readMetadata(final SchemaMetadataInfo schemaMeta, final ExaMetadata meta)
            throws SQLException, AdapterException {
        final List<String> tables = JdbcAdapterProperties.getTableFilter(schemaMeta.getProperties());
        return readMetadata(schemaMeta, tables, meta);
    }

    private static SchemaMetadata readMetadata(final SchemaMetadataInfo meta, final List<String> tables,
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
                JdbcAdapterProperties.getIgnoreErrorList(meta.getProperties()));
    }

    private static String handleRefresh(final RefreshRequest request, final ExaMetadata meta)
            throws SQLException, AdapterException {
        SchemaMetadata remoteMeta;
        JdbcAdapterProperties.checkPropertyConsistency(request.getSchemaMetadataInfo().getProperties());
        if (request.isRefreshForTables()) {
            final List<String> tables = request.getTables();
            remoteMeta = readMetadata(request.getSchemaMetadataInfo(), tables, meta);
        } else {
            remoteMeta = readMetadata(request.getSchemaMetadataInfo(), meta);
        }
        return ResponseJsonSerializer.makeRefreshResponse(remoteMeta);
    }

    private static String handleSetProperty(final SetPropertiesRequest request, final ExaMetadata exaMeta)
            throws SQLException, AdapterException {
        final Map<String, String> changedProperties = request.getProperties();
        final Map<String, String> newSchemaMeta = JdbcAdapterProperties
                .getNewProperties(request.getSchemaMetadataInfo().getProperties(), changedProperties);
        JdbcAdapterProperties.checkPropertyConsistency(newSchemaMeta);
        if (JdbcAdapterProperties.isRefreshNeeded(changedProperties)) {
            final ExaConnectionInformation connection = JdbcAdapterProperties.getConnectionInformation(newSchemaMeta,
                    exaMeta);
            final List<String> tableFilter = JdbcAdapterProperties.getTableFilter(newSchemaMeta);
            final SchemaMetadata remoteMeta = JdbcMetadataReader.readRemoteMetadata(connection.getAddress(),
                    connection.getUser(), connection.getPassword(), JdbcAdapterProperties.getCatalog(newSchemaMeta),
                    JdbcAdapterProperties.getSchema(newSchemaMeta), tableFilter,
                    JdbcAdapterProperties.getSqlDialectName(newSchemaMeta),
                    JdbcAdapterProperties.getExceptionHandlingMode(newSchemaMeta),
                    JdbcAdapterProperties.getIgnoreErrorList(newSchemaMeta));
            return ResponseJsonSerializer.makeSetPropertiesResponse(remoteMeta);
        }
        return ResponseJsonSerializer.makeSetPropertiesResponse(null);
    }

    private static String handleDropVirtualSchema(final DropVirtualSchemaRequest request) {
        return ResponseJsonSerializer.makeDropVirtualSchemaResponse();
    }

    public static String handleGetCapabilities(final GetCapabilitiesRequest request) throws AdapterException {
        final SqlDialectContext dialectContext = new SqlDialectContext(SchemaAdapterNotes.deserialize(
                request.getSchemaMetadataInfo().getAdapterNotes(), request.getSchemaMetadataInfo().getSchemaName()));
        final SqlDialect dialect = JdbcAdapterProperties.getSqlDialect(request.getSchemaMetadataInfo().getProperties(),
                dialectContext);
        final Capabilities capabilities = dialect.getCapabilities();
        final Capabilities excludedCapabilities = parseExcludedCapabilities(
                JdbcAdapterProperties.getExcludedCapabilities(request.getSchemaMetadataInfo().getProperties()));
        capabilities.subtractCapabilities(excludedCapabilities);
        return ResponseJsonSerializer.makeGetCapabilitiesResponse(capabilities);
    }

    private static Capabilities parseExcludedCapabilities(final String excludedCapabilitiesStr) {
        logger.info(() -> "Excluded Capabilities: "
                + (excludedCapabilitiesStr.isEmpty() ? "none" : excludedCapabilitiesStr));
        final Capabilities excludedCapabilities = new Capabilities();
        for (String capability : excludedCapabilitiesStr.split(",")) {
            capability = capability.trim();
            if (capability.isEmpty()) {
                continue;
            }
            if (capability.startsWith(ResponseJsonSerializer.LITERAL_PREFIX)) {
                final String literalCap = capability.replaceFirst(ResponseJsonSerializer.LITERAL_PREFIX, "");
                excludedCapabilities.supportLiteral(LiteralCapability.valueOf(literalCap));
            } else if (capability.startsWith(ResponseJsonSerializer.AGGREGATE_FUNCTION_PREFIX)) {
                // Aggregate functions must be checked before scalar functions
                final String aggregateFunctionCap = capability.replaceFirst(ResponseJsonSerializer.AGGREGATE_FUNCTION_PREFIX,
                        "");
                excludedCapabilities
                        .supportAggregateFunction(AggregateFunctionCapability.valueOf(aggregateFunctionCap));
            } else if (capability.startsWith(ResponseJsonSerializer.SCALAR_FUNCTION_PREFIX)) {
                final String scalarFunctionCap = capability.replaceFirst(ResponseJsonSerializer.SCALAR_FUNCTION_PREFIX, "");
                excludedCapabilities.supportScalarFunction(ScalarFunctionCapability.valueOf(scalarFunctionCap));
            } else {
                // High Level Capability
                excludedCapabilities.supportMainCapability(MainCapability.valueOf(capability));
            }
        }
        return excludedCapabilities;
    }

    private static String handlePushdownRequest(final PushdownRequest request, final ExaMetadata exaMeta)
            throws AdapterException {
        // Generate SQL pushdown query
        final SchemaMetadataInfo meta = request.getSchemaMetadataInfo();
        final SqlDialectContext dialectContext = new SqlDialectContext(SchemaAdapterNotes.deserialize(
                request.getSchemaMetadataInfo().getAdapterNotes(), request.getSchemaMetadataInfo().getSchemaName()));
        final SqlDialect dialect = JdbcAdapterProperties.getSqlDialect(request.getSchemaMetadataInfo().getProperties(),
                dialectContext);
        final boolean hasMoreThanOneTable = request.getInvolvedTablesMetadata().size() > 1;
        final SqlGenerationContext context = new SqlGenerationContext(
                JdbcAdapterProperties.getCatalog(meta.getProperties()),
                JdbcAdapterProperties.getSchema(meta.getProperties()),
                JdbcAdapterProperties.isLocal(meta.getProperties()),
                hasMoreThanOneTable);
        final SqlGenerationVisitor sqlGeneratorVisitor = dialect.getSqlGenerationVisitor(context);
        final String pushdownQuery = request.getSelect().accept(sqlGeneratorVisitor);

        String sql = generateImportQueryForPushdownQuery(exaMeta, meta, dialect, pushdownQuery);

        return ResponseJsonSerializer.makePushdownResponse(sql);
    }

    private static String generateImportQueryForPushdownQuery(ExaMetadata exaMeta, SchemaMetadataInfo meta, SqlDialect dialect, String pushdownQuery) throws AdapterException {
        String sql = "";
        if (JdbcAdapterProperties.isLocal(meta.getProperties())) {
            sql = generateLocalQuery(pushdownQuery);
        } else if (JdbcAdapterProperties.isImportFromExa(meta.getProperties())) {
            sql = generateExasolImportQuery(exaMeta, meta, pushdownQuery);
        } else if (JdbcAdapterProperties.isImportFromOra(meta.getProperties())) {
            sql = generateOracleImportQuery(exaMeta, meta, pushdownQuery);
        } else {
            sql = generateJDBCImportQuery(exaMeta, meta, dialect, pushdownQuery);
        }
        return sql;
    }

    private static String generateLocalQuery(final String pushdownQuery) {
        return pushdownQuery;
    }

    private static String generateExasolImportQuery(ExaMetadata exaMeta, SchemaMetadataInfo meta, String pushdownQuery) {
        String credentials = getCredentialsForEXAImport(exaMeta, meta);
        StringBuilder exasolImportQuery = new StringBuilder();
        exasolImportQuery.append("IMPORT FROM EXA AT '");
        exasolImportQuery.append(JdbcAdapterProperties.getExaConnectionString(meta.getProperties()));
        exasolImportQuery.append("' ");
        exasolImportQuery.append(credentials);
        exasolImportQuery.append(" STATEMENT '");
        exasolImportQuery.append( pushdownQuery.replace("'", "''"));
        exasolImportQuery.append("'");
        return exasolImportQuery.toString();
    }

    private static String generateOracleImportQuery(ExaMetadata exaMeta, SchemaMetadataInfo meta, String pushdownQuery) {
        String credentials = getCredentialsForORAImport(exaMeta, meta);
        StringBuilder oracleImportQuery = new StringBuilder();
        oracleImportQuery.append("IMPORT FROM ORA AT ");
        oracleImportQuery.append(JdbcAdapterProperties.getOraConnectionName(meta.getProperties()));
        oracleImportQuery.append(" ");
        oracleImportQuery.append(credentials);
        oracleImportQuery.append(" STATEMENT '");
        oracleImportQuery.append( pushdownQuery.replace("'", "''"));
        oracleImportQuery.append("'");
        return oracleImportQuery.toString();
    }

    private static String generateJDBCImportQuery(ExaMetadata exaMeta, SchemaMetadataInfo meta, SqlDialect dialect, String pushdownQuery) throws AdapterException {
        String credentials = getCredentialsForJDBCImport(exaMeta, meta);

        StringBuilder jdbcImportQuery = new StringBuilder();
        final String columnDescription = createColumnDescription(exaMeta, meta, pushdownQuery, dialect);
        if (columnDescription == null) {
            jdbcImportQuery.append("IMPORT FROM JDBC AT ");
            jdbcImportQuery.append(credentials);
            jdbcImportQuery.append(" STATEMENT '");
            jdbcImportQuery.append( pushdownQuery.replace("'", "''"));
            jdbcImportQuery.append("'");
        } else {
            jdbcImportQuery.append("IMPORT INTO ");
            jdbcImportQuery.append(columnDescription);
            jdbcImportQuery.append(" FROM JDBC AT ");
            jdbcImportQuery.append(credentials);
            jdbcImportQuery.append(" STATEMENT '");
            jdbcImportQuery.append( pushdownQuery.replace("'", "''"));
            jdbcImportQuery.append("'");
        }
        return jdbcImportQuery.toString();
    }

    protected static String getCredentialsForJDBCImport(ExaMetadata exaMeta, SchemaMetadataInfo meta) {
        String credentials = "";
        if (JdbcAdapterProperties.isUserSpecifiedConnection(meta.getProperties())) {
            credentials = JdbcAdapterProperties.getConnectionName(meta.getProperties());
        } else {
            credentials = getUserAndPasswordForImport(exaMeta, meta);
            final ExaConnectionInformation connection = JdbcAdapterProperties.getConnectionInformation(meta.getProperties(), exaMeta);
            credentials = "'" + connection.getAddress() + "' " + credentials;
        }
        return credentials;
    }

    protected static String getCredentialsForORAImport(ExaMetadata exaMeta, SchemaMetadataInfo meta) {
        String credentials = "";
        if (!JdbcAdapterProperties.isUserSpecifiedConnection(meta.getProperties())) {
            credentials = getUserAndPasswordForImport(exaMeta, meta);
        }
        return credentials;
    }

    protected static String getCredentialsForEXAImport(ExaMetadata exaMeta, SchemaMetadataInfo meta) {
        return getUserAndPasswordForImport(exaMeta, meta);
    }

    private static String getUserAndPasswordForImport(ExaMetadata exaMeta, SchemaMetadataInfo meta) {
        String credentials = "";
        final ExaConnectionInformation connection = JdbcAdapterProperties.getConnectionInformation(meta.getProperties(),
                    exaMeta);
        if (connection.getUser() != null || connection.getPassword() != null) {
            credentials = "USER '" + connection.getUser() + "' IDENTIFIED BY '" + connection.getPassword() + "'";
        }
        return credentials;
    }

    private static String createColumnDescription(final ExaMetadata exaMeta, final SchemaMetadataInfo meta,
            final String pushdownQuery, final SqlDialect dialect) throws AdapterException {
        PreparedStatement ps = null;
        final ExaConnectionInformation connectionInformation = JdbcAdapterProperties
                .getConnectionInformation(meta.getProperties(), exaMeta);

        Connection connection = null;
        try {
            connection = establishConnection(connectionInformation);
            logger.fine(() -> "createColumnDescription: " + pushdownQuery);
            ps = connection.prepareStatement(pushdownQuery);
            ResultSetMetaData metadata = ps.getMetaData();
            if (metadata == null) {
                ps.execute();
                metadata = ps.getMetaData();
                if (metadata == null) {
                    throw new SQLException("getMetaData() failed");
                }
            }
            final DataType[] internalTypes = new DataType[metadata.getColumnCount()];
            for (int col = 1; col <= metadata.getColumnCount(); ++col) {
                final int jdbcType = metadata.getColumnType(col);
                final int jdbcPrecisions = metadata.getPrecision(col);
                final int jdbcScales = metadata.getScale(col);
                final JdbcTypeDescription description = new JdbcTypeDescription(jdbcType, jdbcScales, jdbcPrecisions, 0,
                        metadata.getColumnTypeName(col));
                internalTypes[col - 1] = dialect.mapJdbcType(description);
            }
            final StringBuffer buffer = new StringBuffer();
            buffer.append('(');
            for (int i = 0; i < internalTypes.length; i++) {
                buffer.append("c");
                buffer.append(i);
                buffer.append(" ");
                buffer.append(internalTypes[i].toString());
                if (i < internalTypes.length - 1) {
                    buffer.append(",");
                }
            }

            buffer.append(')');
            return buffer.toString();
        } catch (final SQLException e) {
            throw new RuntimeException("Cannot resolve column types.", e);
        }
    }

    private static Connection establishConnection(final ExaConnectionInformation connection) throws SQLException {
        final String connectionString = connection.getAddress();
        final String user = connection.getUser();
        final String password = connection.getPassword();
        logger.fine(() -> "Connection parameters: " + connectionString);

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

    // Forward stdout to an external output service
    private static OutputStream tryAttachToOutputService(final SchemaMetadataInfo meta) throws AdapterException {
        final String debugAddress = JdbcAdapterProperties.getDebugAddress(meta.getProperties());
        if (!debugAddress.isEmpty()) {
            try {
                final String debugHost = debugAddress.split(":")[0];
                final int debugPort = Integer.parseInt(debugAddress.split(":")[1]);
                return UdfUtils.tryAttachToOutputService(debugHost, debugPort);
            } catch (final Exception ex) {
                throw new AdapterException(
                        "You have to specify a valid hostname and port for the udf debug service, e.g. 'hostname:3000'");
            }
        }
        return null;
    }
}