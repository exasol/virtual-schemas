package com.exasol.adapter.dialects.oracle;

import static com.exasol.adapter.AdapterProperties.*;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;

import java.sql.Connection;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.ConnectionInformation;
import com.exasol.adapter.jdbc.RemoteMetadataReader;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.sql.AggregateFunction;
import com.exasol.adapter.sql.ScalarFunction;

/**
 * This class implements the Oracle SQL dialect
 */
public class OracleSqlDialect extends AbstractSqlDialect {
    private static final String NAME = "ORACLE";
    public static final String ORACLE_CAST_NUMBER_TO_DECIMAL_PROPERTY = "ORACLE_CAST_NUMBER_TO_DECIMAL_WITH_PRECISION_AND_SCALE";
    public static final String ORACLE_IMPORT_PROPERTY = "IMPORT_FROM_ORA";
    public static final String ORACLE_CONNECTION_NAME_PROPERTY = "ORA_CONNECTION_NAME";
    private static final List<String> SUPPORTED_PROPERTIES = Arrays.asList(SQL_DIALECT_PROPERTY,
            CONNECTION_NAME_PROPERTY, CONNECTION_STRING_PROPERTY, USERNAME_PROPERTY, PASSWORD_PROPERTY,
            SCHEMA_NAME_PROPERTY, TABLE_FILTER_PROPERTY, ORACLE_IMPORT_PROPERTY, ORACLE_CONNECTION_NAME_PROPERTY,
            EXCLUDED_CAPABILITIES_PROPERTY, ORACLE_CAST_NUMBER_TO_DECIMAL_PROPERTY, DEBUG_ADDRESS_PROPERTY,
            LOG_LEVEL_PROPERTY);

    public OracleSqlDialect(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
        this.omitParenthesesMap.add(ScalarFunction.SYSDATE);
        this.omitParenthesesMap.add(ScalarFunction.SYSTIMESTAMP);
    }

    public static String getPublicName() {
        return NAME;
    }

    @Override
    public Capabilities getCapabilities() {
        final Capabilities.Builder builder = Capabilities.builder();
        builder.addMain(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS, AGGREGATE_SINGLE_GROUP,
                AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION, AGGREGATE_GROUP_BY_TUPLE, AGGREGATE_HAVING,
                ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT, LIMIT_WITH_OFFSET);
        builder.addPredicate(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, LIKE_ESCAPE, REGEXP_LIKE, BETWEEN,
                IN_CONSTLIST, IS_NULL, IS_NOT_NULL);
        builder.addLiteral(NULL, DATE, TIMESTAMP, TIMESTAMP_UTC, DOUBLE, EXACTNUMERIC, STRING, INTERVAL);
        builder.addAggregateFunction(COUNT, COUNT_STAR, COUNT_DISTINCT, GROUP_CONCAT, GROUP_CONCAT_SEPARATOR,
                GROUP_CONCAT_ORDER_BY);
        builder.addAggregateFunction(SUM, SUM_DISTINCT, MIN, MAX, AVG, AVG_DISTINCT, MEDIAN, FIRST_VALUE, //
                LAST_VALUE, STDDEV, STDDEV_DISTINCT, STDDEV_POP, STDDEV_SAMP, VARIANCE, VARIANCE_DISTINCT, VAR_POP,
                VAR_SAMP);
        builder.addScalarFunction(CEIL, DIV, FLOOR, SIGN);
        builder.addScalarFunction(ADD, SUB, MULT, FLOAT_DIV, NEG, ABS, ACOS, ASIN, ATAN, ATAN2, COS, COSH, COT, DEGREES,
                EXP, GREATEST, LEAST, LN, LOG, MOD, POWER, RADIANS, SIN, SINH, SQRT, TAN, TANH);
        builder.addScalarFunction(ASCII, CHR, INSTR, LENGTH, LOCATE, LOWER, LPAD, LTRIM, REGEXP_INSTR, REGEXP_REPLACE,
                REGEXP_SUBSTR, REPEAT, REPLACE, REVERSE, RPAD, RTRIM, SOUNDEX, SUBSTR, TRANSLATE, TRIM, UPPER, ADD_DAYS,
                ADD_HOURS, ADD_MINUTES, ADD_MONTHS, ADD_SECONDS, ADD_WEEKS, ADD_YEARS, CURRENT_DATE, CURRENT_TIMESTAMP,
                DBTIMEZONE, LOCALTIMESTAMP, NUMTODSINTERVAL, NUMTOYMINTERVAL, SESSIONTIMEZONE, SYSDATE, SYSTIMESTAMP,
                CAST, TO_CHAR, TO_DATE, TO_DSINTERVAL, TO_YMINTERVAL, TO_NUMBER, TO_TIMESTAMP, BIT_AND, BIT_TO_NUM,
                CASE, NULLIFZERO, ZEROIFNULL);
        return builder.build();
    }

    @Override
    public Map<AggregateFunction, String> getAggregateFunctionAliases() {
        return new EnumMap<>(AggregateFunction.class);
    }

    @Override
    public StructureElementSupport supportsJdbcCatalogs() {
        return StructureElementSupport.NONE;
    }

    @Override
    public StructureElementSupport supportsJdbcSchemas() {
        return StructureElementSupport.MULTIPLE;
    }

    DataType getOracleNumberTargetType() {
        if (this.properties.containsKey(ORACLE_CAST_NUMBER_TO_DECIMAL_PROPERTY)) {
            return getOracleNumberTypeFromProperty();
        } else {
            return DataType.createMaximumSizeVarChar(DataType.ExaCharset.UTF8);
        }
    }

    private DataType getOracleNumberTypeFromProperty() {
        final String oraclePrecisionAndScale = this.properties.get(ORACLE_CAST_NUMBER_TO_DECIMAL_PROPERTY);
        final List<String> precisionAndScaleList = Arrays.stream(oraclePrecisionAndScale.split(",")).map(String::trim)
                .collect(Collectors.toList());
        return DataType.createDecimal(Integer.valueOf(precisionAndScaleList.get(0)),
                Integer.valueOf(precisionAndScaleList.get(1)));
    }

    @Override
    public SqlGenerationVisitor getSqlGenerationVisitor(final SqlGenerationContext context) {
        return new OracleSqlGenerationVisitor(this, context);
    }

    @Override
    public String applyQuote(final String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    @Override
    public boolean requiresCatalogQualifiedTableNames(final SqlGenerationContext context) {
        return false;
    }

    @Override
    public boolean requiresSchemaQualifiedTableNames(final SqlGenerationContext context) {
        return true;
    }

    @Override
    public NullSorting getDefaultNullSorting() {
        return NullSorting.NULLS_SORTED_HIGH;
    }

    @Override
    public String getStringLiteral(final String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    @Override
    public String generatePushdownSql(final ConnectionInformation connectionInformation, final String columnDescription,
            final String pushdownSql) {
        final ImportType importType = getImportType();
        if (importType == ImportType.JDBC) {
            return super.generatePushdownSql(connectionInformation, columnDescription, pushdownSql);
        } else {
            if ((importType != ImportType.ORA)) {
                throw new AssertionError("OracleSqlDialect has wrong ImportType");
            }
            final StringBuilder oracleImportQuery = new StringBuilder();
            oracleImportQuery.append("IMPORT FROM ORA AT ").append(connectionInformation.getOraConnectionName())
                    .append(" ");
            oracleImportQuery.append(connectionInformation.getCredentials());
            oracleImportQuery.append(" STATEMENT '").append(pushdownSql.replace("'", "''")).append("'");
            return oracleImportQuery.toString();
        }
    }

    /**
     * Return the type of import the Oracle dialect uses
     *
     * @return import type
     */
    public ImportType getImportType() {
        if (this.properties.isEnabled(IS_LOCAL_PROPERTY)) {
            return ImportType.LOCAL;
        } else if (this.properties.isEnabled(ORACLE_IMPORT_PROPERTY)) {
            return ImportType.ORA;
        } else {
            return ImportType.JDBC;
        }
    }

    @Override
    protected RemoteMetadataReader createRemoteMetadataReader() {
        return new OracleMetadataReader(this.connection, this.properties);
    }

    @Override
    public void validateProperties() throws PropertyValidationException {
        super.validateDialectName(getPublicName());
        super.validateProperties();
        super.checkImportPropertyConsistency(ORACLE_IMPORT_PROPERTY, ORACLE_CONNECTION_NAME_PROPERTY);
        super.validateBooleanProperty(ORACLE_IMPORT_PROPERTY);
        validateCastNumberToDecimalProperty();
    }

    @Override
    protected List<String> getSupportedProperties() {
        return SUPPORTED_PROPERTIES;
    }

    private void validateCastNumberToDecimalProperty() throws PropertyValidationException {
        if (this.properties.containsKey(ORACLE_CAST_NUMBER_TO_DECIMAL_PROPERTY)) {
            final Pattern pattern = Pattern.compile("\\s*(\\d+)\\s*,\\s*(\\d+)\\s*");
            final String oraclePrecisionAndScale = this.properties.get(ORACLE_CAST_NUMBER_TO_DECIMAL_PROPERTY);
            final Matcher matcher = pattern.matcher(oraclePrecisionAndScale);
            if (!matcher.matches()) {
                throw new PropertyValidationException("Unable to parse adapter property "
                        + ORACLE_CAST_NUMBER_TO_DECIMAL_PROPERTY + " value \"" + oraclePrecisionAndScale
                        + " into a number precison and scale. The required format is \"<precsion>.<scale>\", where "
                        + "both are integer numbers.");
            }
        }
    }
}
