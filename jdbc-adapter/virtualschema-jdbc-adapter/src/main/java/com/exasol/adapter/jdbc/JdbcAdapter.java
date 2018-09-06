package com.exasol.adapter.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

import com.exasol.ExaConnectionInformation;
import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.capabilities.AggregateFunctionCapability;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.capabilities.LiteralCapability;
import com.exasol.adapter.capabilities.MainCapability;
import com.exasol.adapter.capabilities.ScalarFunctionCapability;
import com.exasol.adapter.dialects.JdbcTypeDescription;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlDialectContext;
import com.exasol.adapter.dialects.SqlGenerationContext;
import com.exasol.adapter.dialects.SqlGenerationVisitor;
import com.exasol.adapter.json.RequestJsonParser;
import com.exasol.adapter.json.ResponseJsonSerializer;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.metadata.SchemaMetadataInfo;
import com.exasol.adapter.request.AdapterRequest;
import com.exasol.adapter.request.CreateVirtualSchemaRequest;
import com.exasol.adapter.request.DropVirtualSchemaRequest;
import com.exasol.adapter.request.GetCapabilitiesRequest;
import com.exasol.adapter.request.PushdownRequest;
import com.exasol.adapter.request.RefreshRequest;
import com.exasol.adapter.request.SetPropertiesRequest;
import com.exasol.utils.JsonHelper;
import com.exasol.utils.UdfUtils;

public class JdbcAdapter {
    public static final int MAX_STRING_CHAR_LENGTH = 2000000;
    public static final Logger LOGGER = Logger.getLogger(JdbcAdapter.class.getName());

    /**
     * This method gets called by the database during interactions with the virtual
     * schema.
     *
     * @param meta  Metadata object
     * @param input JSON request, as defined in the Adapter Script API
     * @return JSON response, as defined in the Adapter Script API
     */
    public static String adapterCall(final ExaMetadata meta, final String input) throws Exception {
        String result = "";
        try {
            final AdapterRequest request = new RequestJsonParser().parseRequest(input);
            tryAttachToOutputService(request.getSchemaMetadataInfo());
            LOGGER.fine(() -> "----------\nAdapter Request:\n----------\n" + input);

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
            LOGGER.fine(
                    "----------\nResponse:\n----------\n" + JsonHelper.prettyJson(JsonHelper.getJsonObject(result)));
            return result;
        } catch (final AdapterException ex) {
            throw ex;
        } catch (final Exception ex) {
            final String stacktrace = UdfUtils.traceToString(ex);
            throw new Exception("Unexpected error in adapter: " + ex.getMessage() + "\nStacktrace: " + stacktrace
                    + "\nFor following request: " + input + "\nResponse: " + result);
        }
    }

    private static String handleCreateVirtualSchema(final CreateVirtualSchemaRequest request, final ExaMetadata meta)
            throws SQLException, AdapterException {
        final Map<String, String> properties = request.getSchemaMetadataInfo().getProperties();
        setLogLevel(properties);
        JdbcAdapterProperties.checkPropertyConsistency(properties);
        final SchemaMetadata remoteMeta = readMetadata(request.getSchemaMetadataInfo(), meta);
        return ResponseJsonSerializer.makeCreateVirtualSchemaResponse(remoteMeta);
    }

    private static void setLogLevel(final Map<String, String> properties) throws InvalidPropertyException {
        final Level logLevel = JdbcAdapterProperties.getLogLevel(properties);
        // System.setProperty("handlers", "java.util.logging.Streamhandler");
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS.%1$tL %4$-7s [%3$s] %5$s %6$s%n");
        // System.setProperty("java.util.logging.StreamHandler.level",
        // logLevel.toString());
        final Logger logger = Logger.getLogger("com.exasol");
        final SimpleFormatter formatter = new SimpleFormatter();
        final StreamHandler handler = new StreamHandler(System.out, formatter);
        handler.setFormatter(formatter);
        logger.addHandler(handler);
        logger.setLevel(logLevel);
        LOGGER.info(() -> "Virtual Schema Adapter - log level \"" + logLevel.toString() + "\"");
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
                JdbcAdapterProperties.getExceptionHandlingMode(meta.getProperties()));
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
                    JdbcAdapterProperties.getExceptionHandlingMode(newSchemaMeta));
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
        System.out.println("Excluded Capabilities: " + excludedCapabilitiesStr);
        final Capabilities excludedCapabilities = new Capabilities();
        for (final String cap : excludedCapabilitiesStr.split(",")) {
            if (cap.trim().isEmpty()) {
                continue;
            }
            if (cap.startsWith(ResponseJsonSerializer.LITERAL_PREFIX)) {
                final String literalCap = cap.replaceFirst(ResponseJsonSerializer.LITERAL_PREFIX, "");
                excludedCapabilities.supportLiteral(LiteralCapability.valueOf(literalCap));
            } else if (cap.startsWith(ResponseJsonSerializer.AGGREGATE_FUNCTION_PREFIX)) {
                // Aggregate functions must be checked before scalar functions
                final String aggregateFunctionCap = cap.replaceFirst(ResponseJsonSerializer.AGGREGATE_FUNCTION_PREFIX,
                        "");
                excludedCapabilities
                        .supportAggregateFunction(AggregateFunctionCapability.valueOf(aggregateFunctionCap));
            } else if (cap.startsWith(ResponseJsonSerializer.SCALAR_FUNCTION_PREFIX)) {
                final String scalarFunctionCap = cap.replaceFirst(ResponseJsonSerializer.SCALAR_FUNCTION_PREFIX, "");
                excludedCapabilities.supportScalarFunction(ScalarFunctionCapability.valueOf(scalarFunctionCap));
            } else {
                // High Level Capability
                excludedCapabilities.supportMainCapability(MainCapability.valueOf(cap));
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
        final SqlGenerationContext context = new SqlGenerationContext(
                JdbcAdapterProperties.getCatalog(meta.getProperties()),
                JdbcAdapterProperties.getSchema(meta.getProperties()),
                JdbcAdapterProperties.isLocal(meta.getProperties()));
        final SqlGenerationVisitor sqlGeneratorVisitor = dialect.getSqlGenerationVisitor(context);
        final String pushdownQuery = request.getSelect().accept(sqlGeneratorVisitor);

        final ExaConnectionInformation connection = JdbcAdapterProperties.getConnectionInformation(meta.getProperties(),
                exaMeta);
        String credentials = "";
        if (connection.getUser() != null || connection.getPassword() != null) {
            credentials = "USER '" + connection.getUser() + "' IDENTIFIED BY '" + connection.getPassword() + "'";
        }

        String sql = "";
        if (JdbcAdapterProperties.isLocal(meta.getProperties())) {
            sql = pushdownQuery;
        } else if (JdbcAdapterProperties.isImportFromExa(meta.getProperties())) {
            sql = String.format("IMPORT FROM EXA AT '%s' %s STATEMENT '%s'",
                    JdbcAdapterProperties.getExaConnectionString(meta.getProperties()), credentials,
                    pushdownQuery.replace("'", "''"));
        } else if (JdbcAdapterProperties.isImportFromOra(meta.getProperties())) {
            sql = String.format("IMPORT FROM ORA AT %s %s STATEMENT '%s'",
                    JdbcAdapterProperties.getOraConnectionName(meta.getProperties()), credentials,
                    pushdownQuery.replace("'", "''"));
        } else {
            if (JdbcAdapterProperties.userSpecifiedConnection(meta.getProperties())) {
                credentials = JdbcAdapterProperties.getConnectionName(meta.getProperties());
            } else {
                credentials = "'" + connection.getAddress() + "' " + credentials;
            }

            final String columnDescription = createColumnDescription(exaMeta, meta, pushdownQuery, dialect);
            if (columnDescription == null) {
                sql = String.format("IMPORT FROM JDBC AT %s STATEMENT '%s'", credentials,
                        pushdownQuery.replace("'", "''"));
            } else {
                sql = String.format("IMPORT INTO %s FROM JDBC AT %s STATEMENT '%s'", columnDescription, credentials,
                        pushdownQuery.replace("'", "''"));
            }
        }

        return ResponseJsonSerializer.makePushdownResponse(sql);
    }

    private static String createColumnDescription(final ExaMetadata exaMeta, final SchemaMetadataInfo meta,
            final String pushdownQuery, final SqlDialect dialect) throws AdapterException {
        PreparedStatement ps = null;
        final ExaConnectionInformation connectionInformation = JdbcAdapterProperties
                .getConnectionInformation(meta.getProperties(), exaMeta);

        Connection connection = null;
        try {
            connection = establishConnection(connectionInformation);
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
            throw new RuntimeException("Cannot resolve column types." + e.getMessage());

        }
    }

    private static Connection establishConnection(final ExaConnectionInformation connection) throws SQLException {
        final String connectionString = connection.getAddress();
        final String user = connection.getUser();
        final String password = connection.getPassword();
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

    // Forward stdout to an external output service
    private static void tryAttachToOutputService(final SchemaMetadataInfo meta) throws AdapterException {
        final String debugAddress = JdbcAdapterProperties.getDebugAddress(meta.getProperties());
        if (!debugAddress.isEmpty()) {
            try {
                final String debugHost = debugAddress.split(":")[0];
                final int debugPort = Integer.parseInt(debugAddress.split(":")[1]);
                UdfUtils.tryAttachToOutputService(debugHost, debugPort);
            } catch (final Exception ex) {
                throw new AdapterException(
                        "You have to specify a valid hostname and port for the udf debug service, e.g. 'hostname:3000'");
            }
        }
    }

}
