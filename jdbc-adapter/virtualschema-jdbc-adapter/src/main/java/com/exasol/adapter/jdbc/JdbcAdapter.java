package com.exasol.adapter.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.RequestDispatcher;
import com.exasol.adapter.VirtualSchemaAdapter;
import com.exasol.adapter.capabilities.AggregateFunctionCapability;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.capabilities.LiteralCapability;
import com.exasol.adapter.capabilities.MainCapability;
import com.exasol.adapter.capabilities.ScalarFunctionCapability;
import com.exasol.adapter.dialects.PropertyValidationException;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlDialectFactory;
import com.exasol.adapter.dialects.SqlDialectRegistry;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.metadata.SchemaMetadataInfo;
import com.exasol.adapter.request.AdapterRequest;
import com.exasol.adapter.request.CreateVirtualSchemaRequest;
import com.exasol.adapter.request.DropVirtualSchemaRequest;
import com.exasol.adapter.request.GetCapabilitiesRequest;
import com.exasol.adapter.request.PushDownRequest;
import com.exasol.adapter.request.RefreshRequest;
import com.exasol.adapter.request.SetPropertiesRequest;
import com.exasol.adapter.response.CreateVirtualSchemaResponse;
import com.exasol.adapter.response.DropVirtualSchemaResponse;
import com.exasol.adapter.response.GetCapabilitiesResponse;
import com.exasol.adapter.response.PushDownResponse;
import com.exasol.adapter.response.RefreshResponse;
import com.exasol.adapter.response.SetPropertiesResponse;

public class JdbcAdapter implements VirtualSchemaAdapter {
    private static final String SCALAR_FUNCTION_PREFIX = "FN_";
    private static final String PREDICATE_PREFIX = "FN_PRED_";
    private static final String AGGREGATE_FUNCTION_PREFIX = "FN_AGG_";
    private static final String LITERAL_PREFIX = "LITERAL_";

    private static final String TABLES_PROPERTY = "TABLE_FILTER";

    private static final Logger LOGGER = Logger.getLogger(JdbcAdapter.class.getName());
    private final RemoteConnectionFactory connectionFactory = new RemoteConnectionFactory();

    /**
     * This method gets called by the database during interactions with the virtual
     * schema.
     *
     * @param metadata   Metadata object
     * @param rawRequest JSON request, as defined in the Adapter Script API
     * @return JSON response, as defined in the Adapter Script API
     * @throws AdapterException in case the request is not recognized
     * @deprecated As of Virtual Schema version 1.8.0 you should use
     *             {@link com.exasol.adapter.RequestDispatcher#adapterCall(ExaMetadata, String)}
     *             as entry point instead.
     */
    @Deprecated
    public static String adapterCall(final ExaMetadata metadata, final String rawRequest) throws AdapterException {
        LOGGER.warning("The adapter entry point \"com.exasol.adapter.jdbc.JdbcAdapter\" is deprecated."
                + " Please use \"com.exasol.adapter.RequestDispatcher\" instead.");
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
        try (final Connection connection = this.connectionFactory.createConnection(exasolMetadata, properties)) {
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
        try (final Connection connection = this.connectionFactory.createConnection(exasolMetadata, properties)) {
            final SqlDialect dialect = createDialect(connection, properties);
            dialect.validateProperties();
            return dialect.readSchemaMetadata(whiteListedRemoteTables);
        }
    }

    private SqlDialect createDialect(final Connection connection, final AdapterProperties properties) {
        final SqlDialectFactory dialectFactory = new SqlDialectFactory(connection, SqlDialectRegistry.getInstance(),
                properties);
        return dialectFactory.createSqlDialect(properties.getSqlDialect());
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
        final Map<String, String> requestRawProperties = request.getProperties();
        final SchemaMetadataInfo schemaMetadataInfo = request.getSchemaMetadataInfo();
        final Map<String, String> mergedRawProperties = mergeProperties(schemaMetadataInfo.getProperties(),
                requestRawProperties);
        final AdapterProperties mergedProperties = new AdapterProperties(mergedRawProperties);
        if (AdapterProperties.isRefreshingVirtualSchemaRequired(requestRawProperties)) {
            final List<String> tableFilter = getTableFilter(mergedRawProperties);
            final SchemaMetadata remoteMeta;
            try {
                remoteMeta = readMetadata(mergedProperties, tableFilter, metadata);
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

    private Map<String, String> mergeProperties(final Map<String, String> previousRawProperties,
            final Map<String, String> requestRawProperties) {
        final Map<String, String> mergedRawProperties = new HashMap<>(previousRawProperties);
        for (final Map.Entry<String, String> requestRawProperty : requestRawProperties.entrySet()) {
            if (requestRawProperty.getValue() == null) {
                mergedRawProperties.remove(requestRawProperty.getKey());
            } else {
                mergedRawProperties.put(requestRawProperty.getKey(), requestRawProperty.getValue());
            }
        }
        return mergedRawProperties;
    }

    private List<String> getTableFilter(final Map<String, String> properties) {
        final String tableNames = properties.get(TABLES_PROPERTY);
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
        final Capabilities excludedCapabilities = getExcludedCapabilities(properties);
        capabilities.subtractCapabilities(excludedCapabilities);
        return GetCapabilitiesResponse //
                .builder()//
                .capabilities(capabilities)//
                .build();
    }

    private Capabilities getExcludedCapabilities(final AdapterProperties properties) {
        if (properties.containsKey(AdapterProperties.EXCLUDED_CAPABILITIES_PROPERTY)) {
            final String excludedCapabilitiesStr = properties.getExcludedCapabilities();
            final Capabilities.Builder builder = parseExcludedCapabilities(excludedCapabilitiesStr);
            return builder.build();
        } else {
            LOGGER.config(() -> "Excluded Capabilities: none");
            return Capabilities.builder().build();
        }
    }

    private Capabilities.Builder parseExcludedCapabilities(final String excludedCapabilitiesString) {
        final Capabilities.Builder builder = Capabilities.builder();
        LOGGER.config(() -> "Excluded Capabilities: "
                + (excludedCapabilitiesString.isEmpty() ? "none" : excludedCapabilitiesString));
        for (String capability : excludedCapabilitiesString.split(",")) {
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
        return builder;
    }

    @Override
    public PushDownResponse pushdown(final ExaMetadata exaMetadata, final PushDownRequest request)
            throws AdapterException {
        final AdapterProperties properties = getPropertiesFromRequest(request);
        try (final Connection connection = this.connectionFactory.createConnection(exaMetadata, properties)) {
            final SqlDialect dialect = createDialect(connection, properties);
            final String importFromPushdownQuery = new ImportQueryBuilder() //
                    .dialect(dialect) //
                    .statement(request.getSelect()) //
                    .properties(properties) //
                    .build();
            return PushDownResponse.builder().pushDownSql(importFromPushdownQuery).build();
        } catch (final SQLException exception) {
            throw new AdapterException("Unable to execute push-down request.", exception);
        }
    }

}