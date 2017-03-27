package com.exasol.adapter.jdbc;

import com.exasol.ExaConnectionInformation;
import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.capabilities.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.dialects.impl.*;
import com.exasol.adapter.json.RequestJsonParser;
import com.exasol.adapter.json.ResponseJsonSerializer;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.metadata.SchemaMetadataInfo;
import com.exasol.adapter.request.*;
import com.exasol.utils.JsonHelper;
import com.exasol.utils.UdfUtils;
import com.google.common.collect.ImmutableList;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class JdbcAdapter {

    final static SqlDialects supportedDialects;
    static {
        supportedDialects = new SqlDialects(
                ImmutableList.of(
                        GenericSqlDialect.NAME,
                        ExasolSqlDialect.NAME,
                        ImpalaSqlDialect.NAME,
                        OracleSqlDialect.NAME,
                        TeradataSqlDialect.NAME,
                        RedshiftSqlDialect.NAME,
                        HiveSqlDialect.NAME,
                        DB2SqlDialect.NAME,
                        SqlServerSqlDialect.NAME,
                        PostgreSQLSqlDialect.NAME));
    }

    /**
     * This method gets called by the database during interactions with the
     * virtual schema.
     *
     * @param meta
     *            Metadata object
     * @param input
     *            json request, as defined in the Adapter Script API
     * @return json response, as defined in the Adapter Script API
     */
    public static String adapterCall(ExaMetadata meta, String input) throws Exception {
        String result = "";
        try {
            AdapterRequest request = new RequestJsonParser().parseRequest(input);
            tryAttachToOutputService(request.getSchemaMetadataInfo());
            System.out.println("----------\nAdapter Request:\n----------\n" + input);
            
            switch (request.getType()) {
            case CREATE_VIRTUAL_SCHEMA:
                result = handleCreateVirtualSchema((CreateVirtualSchemaRequest)request, meta);
                break;
            case DROP_VIRTUAL_SCHEMA:
                result = handleDropVirtualSchema((DropVirtualSchemaRequest)request);
                break;
            case REFRESH:
                result = handleRefresh((RefreshRequest)request, meta);
                break;
            case SET_PROPERTIES:
                result = handleSetProperty((SetPropertiesRequest)request, meta);
                break;
            case GET_CAPABILITIES:
                result = handleGetCapabilities((GetCapabilitiesRequest)request);
                break;
            case PUSHDOWN:
                result = handlePushdownRequest((PushdownRequest)request, meta);
                break;
            default:
                throw new RuntimeException("Request Type not supported: " + request.getType());
            }
            assert(result.isEmpty());
            System.out.println("----------\nResponse:\n----------\n" + JsonHelper.prettyJson(JsonHelper.getJsonObject(result)));
            return result;
        } catch (AdapterException ex) {
            throw ex;
        }
        catch (Exception ex) {
            String stacktrace = UdfUtils.traceToString(ex);
            throw new Exception("Unexpected error in adapter: " + ex.getMessage() + "\nStacktrace: " + stacktrace + "\nFor following request: " + input + "\nResponse: " + result);
        }
    }

    private static String handleCreateVirtualSchema(CreateVirtualSchemaRequest request, ExaMetadata meta) throws SQLException, AdapterException {
        JdbcAdapterProperties.checkPropertyConsistency(request.getSchemaMetadataInfo().getProperties(), supportedDialects);
        SchemaMetadata remoteMeta = readMetadata(request.getSchemaMetadataInfo(), meta);
        return ResponseJsonSerializer.makeCreateVirtualSchemaResponse(remoteMeta);
    }
    
    private static SchemaMetadata readMetadata(SchemaMetadataInfo schemaMeta, ExaMetadata meta) throws SQLException, AdapterException {
        List<String> tables = JdbcAdapterProperties.getTableFilter(schemaMeta.getProperties());
        return readMetadata(schemaMeta, tables, meta);
    }

    private static SchemaMetadata readMetadata(SchemaMetadataInfo meta, List<String> tables, ExaMetadata exaMeta) throws SQLException, AdapterException {
        // Connect via JDBC and read metadata
        ExaConnectionInformation connection = JdbcAdapterProperties.getConnectionInformation(meta.getProperties(), exaMeta);
        String catalog = JdbcAdapterProperties.getCatalog(meta.getProperties());
        String schema = JdbcAdapterProperties.getSchema(meta.getProperties());
        return JdbcMetadataReader.readRemoteMetadata(
                connection.getAddress(),
                connection.getUser(),
                connection.getPassword(),
                catalog,
                schema,
                tables,
                supportedDialects,
                JdbcAdapterProperties.getSqlDialectName(meta.getProperties(), supportedDialects));
    }
    
    private static String handleRefresh(RefreshRequest request, ExaMetadata meta) throws SQLException, AdapterException {
        SchemaMetadata remoteMeta;
        JdbcAdapterProperties.checkPropertyConsistency(request.getSchemaMetadataInfo().getProperties(), supportedDialects);
        if (request.isRefreshForTables()) {
            List<String> tables = request.getTables();
            remoteMeta = readMetadata(request.getSchemaMetadataInfo(), tables, meta);
        } else {
            remoteMeta = readMetadata(request.getSchemaMetadataInfo(), meta);
        }
        return ResponseJsonSerializer.makeRefreshResponse(remoteMeta);
    }

    private static String handleSetProperty(SetPropertiesRequest request, ExaMetadata exaMeta) throws SQLException, AdapterException {
        Map<String, String> changedProperties = request.getProperties();
        Map<String, String> newSchemaMeta = JdbcAdapterProperties.getNewProperties(
                request.getSchemaMetadataInfo().getProperties(), changedProperties);
        JdbcAdapterProperties.checkPropertyConsistency(newSchemaMeta, supportedDialects);
        if (JdbcAdapterProperties.isRefreshNeeded(changedProperties)) {
            ExaConnectionInformation connection = JdbcAdapterProperties.getConnectionInformation(newSchemaMeta, exaMeta);
            List<String> tableFilter = JdbcAdapterProperties.getTableFilter(newSchemaMeta);
            SchemaMetadata remoteMeta = JdbcMetadataReader.readRemoteMetadata(
                    connection.getAddress(),
                    connection.getUser(),
                    connection.getPassword(),
                    JdbcAdapterProperties.getCatalog(newSchemaMeta),
                    JdbcAdapterProperties.getSchema(newSchemaMeta),
                    tableFilter,
                    supportedDialects,
                    JdbcAdapterProperties.getSqlDialectName(newSchemaMeta, supportedDialects));
            return ResponseJsonSerializer.makeSetPropertiesResponse(remoteMeta);
        }
        return ResponseJsonSerializer.makeSetPropertiesResponse(null);
    }

    private static String handleDropVirtualSchema(DropVirtualSchemaRequest request) {
        return ResponseJsonSerializer.makeDropVirtualSchemaResponse();
    }
    
    public static String handleGetCapabilities(GetCapabilitiesRequest request) throws AdapterException {
        SqlDialectContext dialectContext = new SqlDialectContext(SchemaAdapterNotes.deserialize(request.getSchemaMetadataInfo().getAdapterNotes(), request.getSchemaMetadataInfo().getSchemaName()));
        SqlDialect dialect = JdbcAdapterProperties.getSqlDialect(request.getSchemaMetadataInfo().getProperties(), supportedDialects, dialectContext);
        Capabilities capabilities = dialect.getCapabilities();
        Capabilities excludedCapabilities = parseExcludedCapabilities(
                JdbcAdapterProperties.getExcludedCapabilities(request.getSchemaMetadataInfo().getProperties()));
        capabilities.subtractCapabilities(excludedCapabilities);
        return ResponseJsonSerializer.makeGetCapabilitiesResponse(capabilities);
    }
    
    private static Capabilities parseExcludedCapabilities(String excludedCapabilitiesStr) {
        System.out.println("Excluded Capabilities: " + excludedCapabilitiesStr);
        Capabilities excludedCapabilities = new Capabilities();
        for (String cap : excludedCapabilitiesStr.split(",")) {
            if (cap.trim().isEmpty()) {
                continue;
            }
            if (cap.startsWith(ResponseJsonSerializer.LITERAL_PREFIX)) {
                String literalCap = cap.replaceFirst(ResponseJsonSerializer.LITERAL_PREFIX, "");
                excludedCapabilities.supportLiteral(LiteralCapability.valueOf(literalCap));
            } else if (cap.startsWith(ResponseJsonSerializer.AGGREGATE_FUNCTION_PREFIX)) {
                // Aggregate functions must be checked before scalar functions
                String aggregateFunctionCap = cap.replaceFirst(ResponseJsonSerializer.AGGREGATE_FUNCTION_PREFIX, "");
                excludedCapabilities.supportAggregateFunction(AggregateFunctionCapability.valueOf(aggregateFunctionCap));
            } else if (cap.startsWith(ResponseJsonSerializer.SCALAR_FUNCTION_PREFIX)) {
                String scalarFunctionCap = cap.replaceFirst(ResponseJsonSerializer.SCALAR_FUNCTION_PREFIX, "");
                excludedCapabilities.supportScalarFunction(ScalarFunctionCapability.valueOf(scalarFunctionCap));
            } else {
                // High Level Capability
                excludedCapabilities.supportMainCapability(MainCapability.valueOf(cap));
            }
        }
        return excludedCapabilities;
    }

    private static String handlePushdownRequest(PushdownRequest request, ExaMetadata exaMeta) throws AdapterException {
        // Generate SQL pushdown query
        SchemaMetadataInfo meta = request.getSchemaMetadataInfo();
        SqlDialectContext dialectContext = new SqlDialectContext(SchemaAdapterNotes.deserialize(request.getSchemaMetadataInfo().getAdapterNotes(), request.getSchemaMetadataInfo().getSchemaName()));
        SqlDialect dialect = JdbcAdapterProperties.getSqlDialect(request.getSchemaMetadataInfo().getProperties(), supportedDialects, dialectContext);
        SqlGenerationContext context = new SqlGenerationContext(JdbcAdapterProperties.getCatalog(meta.getProperties()), JdbcAdapterProperties.getSchema(meta.getProperties()), JdbcAdapterProperties.isLocal(meta.getProperties()));
        SqlGenerationVisitor sqlGeneratorVisitor = dialect.getSqlGenerationVisitor(context);
        String pushdownQuery = request.getSelect().accept(sqlGeneratorVisitor);

        boolean isLocal = JdbcAdapterProperties.isLocal(meta.getProperties());
        String credentialsAndConn = "";
        if (JdbcAdapterProperties.userSpecifiedConnection(meta.getProperties())) {
            credentialsAndConn = "AT " + JdbcAdapterProperties.getConnectionName(meta.getProperties());
        } else {
            ExaConnectionInformation connection = JdbcAdapterProperties.getConnectionInformation(meta.getProperties(), exaMeta);
            if (JdbcAdapterProperties.isImportFromExa(meta.getProperties())) {
                credentialsAndConn = "AT '" + JdbcAdapterProperties.getExaConnectionString(meta.getProperties()) + "'";
            } else {
                credentialsAndConn = "AT '" + connection.getAddress() + "'";
            }
            credentialsAndConn += " USER '" + connection.getUser() + "'";
            credentialsAndConn += " IDENTIFIED BY '" + connection.getPassword() + "'";
        }
        String importSql;
        boolean importFromExa = JdbcAdapterProperties.isImportFromExa(meta.getProperties());
        if (isLocal) {
            importSql = pushdownQuery;
        } else if (importFromExa) {
            importSql =  "IMPORT FROM EXA " + credentialsAndConn
                    + " STATEMENT '" + pushdownQuery.replace("'", "''") + "'";
        } else {
            importSql =  "IMPORT FROM JDBC " + credentialsAndConn
                    + " STATEMENT '" + pushdownQuery.replace("'", "''") + "'";
        }
        
        return ResponseJsonSerializer.makePushdownResponse(importSql);
    }

    // Forward stdout to an external output service
    private static void tryAttachToOutputService(SchemaMetadataInfo meta) throws AdapterException {
        String debugAddress = JdbcAdapterProperties.getDebugAddress(meta.getProperties());
        if (!debugAddress.isEmpty()) {
            try {
                String debugHost = debugAddress.split(":")[0];
                int debugPort = Integer.parseInt(debugAddress.split(":")[1]);
                UdfUtils.tryAttachToOutputService(debugHost, debugPort);
            } catch (Exception ex) {
                throw new AdapterException("You have to specify a valid hostname and port for the udf debug service, e.g. 'hostname:3000'");
            }
        }
    }

}
