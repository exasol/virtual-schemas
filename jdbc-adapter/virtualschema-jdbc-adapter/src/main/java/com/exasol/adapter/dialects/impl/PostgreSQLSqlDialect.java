package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.capabilities.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.sql.ScalarFunction;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public class PostgreSQLSqlDialect extends AbstractSqlDialect {
    private static final int MAX_POSTGRES_SQL_VARCHAR_SIZE_BASED_ON_EXASOL_LIMIT = 2000000;
    private static final String POSTGRES_IGNORE_UPPERCASE_TABLES = "POSTGRESQL_UPPERCASE_TABLES";
    private static final String NAME = "POSTGRESQL";

    public PostgreSQLSqlDialect(final SqlDialectContext context) {
        super(context);
    }

    public static String getPublicName() {
        return NAME;
    }

    @Override
    public Capabilities getCapabilities() {
        final Capabilities cap = new Capabilities();

        cap.supportMainCapability(MainCapability.SELECTLIST_PROJECTION);
        cap.supportMainCapability(MainCapability.SELECTLIST_EXPRESSIONS);
        cap.supportMainCapability(MainCapability.FILTER_EXPRESSIONS);
        cap.supportMainCapability(MainCapability.AGGREGATE_SINGLE_GROUP);
        cap.supportMainCapability(MainCapability.AGGREGATE_GROUP_BY_COLUMN);
        cap.supportMainCapability(MainCapability.AGGREGATE_GROUP_BY_EXPRESSION);
        cap.supportMainCapability(MainCapability.AGGREGATE_GROUP_BY_TUPLE);
        cap.supportMainCapability(MainCapability.AGGREGATE_HAVING);
        cap.supportMainCapability(MainCapability.ORDER_BY_COLUMN);
        cap.supportMainCapability(MainCapability.ORDER_BY_EXPRESSION);
        cap.supportMainCapability(MainCapability.LIMIT);
        cap.supportMainCapability(MainCapability.LIMIT_WITH_OFFSET);

        cap.supportPredicate(PredicateCapability.AND);
        cap.supportPredicate(PredicateCapability.OR);
        cap.supportPredicate(PredicateCapability.NOT);
        cap.supportPredicate(PredicateCapability.EQUAL);
        cap.supportPredicate(PredicateCapability.NOTEQUAL);
        cap.supportPredicate(PredicateCapability.LESS);
        cap.supportPredicate(PredicateCapability.LESSEQUAL);
        cap.supportPredicate(PredicateCapability.LIKE);
        cap.supportPredicate(PredicateCapability.LIKE_ESCAPE);
        cap.supportPredicate(PredicateCapability.REGEXP_LIKE);
        cap.supportPredicate(PredicateCapability.BETWEEN);
        cap.supportPredicate(PredicateCapability.IN_CONSTLIST);
        cap.supportPredicate(PredicateCapability.IS_NULL);
        cap.supportPredicate(PredicateCapability.IS_NOT_NULL);

        cap.supportLiteral(LiteralCapability.BOOL);
        cap.supportLiteral(LiteralCapability.NULL);
        cap.supportLiteral(LiteralCapability.DATE);
        cap.supportLiteral(LiteralCapability.TIMESTAMP);
        cap.supportLiteral(LiteralCapability.TIMESTAMP_UTC);
        cap.supportLiteral(LiteralCapability.DOUBLE);
        cap.supportLiteral(LiteralCapability.EXACTNUMERIC);
        cap.supportLiteral(LiteralCapability.STRING);

        cap.supportAggregateFunction(AggregateFunctionCapability.COUNT);
        cap.supportAggregateFunction(AggregateFunctionCapability.COUNT_STAR);
        cap.supportAggregateFunction(AggregateFunctionCapability.COUNT_DISTINCT);

        cap.supportAggregateFunction(AggregateFunctionCapability.SUM);
        cap.supportAggregateFunction(AggregateFunctionCapability.SUM_DISTINCT);
        cap.supportAggregateFunction(AggregateFunctionCapability.MIN);
        cap.supportAggregateFunction(AggregateFunctionCapability.MAX);
        cap.supportAggregateFunction(AggregateFunctionCapability.AVG);
        cap.supportAggregateFunction(AggregateFunctionCapability.AVG_DISTINCT);

        cap.supportAggregateFunction(AggregateFunctionCapability.MEDIAN);
        cap.supportAggregateFunction(AggregateFunctionCapability.FIRST_VALUE);
        cap.supportAggregateFunction(AggregateFunctionCapability.LAST_VALUE);

        cap.supportAggregateFunction(AggregateFunctionCapability.STDDEV);
        cap.supportAggregateFunction(AggregateFunctionCapability.STDDEV_DISTINCT);
        cap.supportAggregateFunction(AggregateFunctionCapability.STDDEV_POP);
        cap.supportAggregateFunction(AggregateFunctionCapability.STDDEV_POP_DISTINCT);
        cap.supportAggregateFunction(AggregateFunctionCapability.STDDEV_SAMP);
        cap.supportAggregateFunction(AggregateFunctionCapability.STDDEV_SAMP_DISTINCT);

        cap.supportAggregateFunction(AggregateFunctionCapability.VARIANCE);
        cap.supportAggregateFunction(AggregateFunctionCapability.VARIANCE_DISTINCT);
        cap.supportAggregateFunction(AggregateFunctionCapability.VAR_POP);
        cap.supportAggregateFunction(AggregateFunctionCapability.VAR_POP_DISTINCT);
        cap.supportAggregateFunction(AggregateFunctionCapability.VAR_SAMP);
        cap.supportAggregateFunction(AggregateFunctionCapability.VAR_SAMP_DISTINCT);

        cap.supportAggregateFunction(AggregateFunctionCapability.GROUP_CONCAT);

        cap.supportScalarFunction(ScalarFunctionCapability.ADD);
        cap.supportScalarFunction(ScalarFunctionCapability.SUB);
        cap.supportScalarFunction(ScalarFunctionCapability.MULT);
        cap.supportScalarFunction(ScalarFunctionCapability.FLOAT_DIV);

        cap.supportScalarFunction(ScalarFunctionCapability.NEG);

        cap.supportScalarFunction(ScalarFunctionCapability.ABS);
        cap.supportScalarFunction(ScalarFunctionCapability.ACOS);
        cap.supportScalarFunction(ScalarFunctionCapability.ASIN);
        cap.supportScalarFunction(ScalarFunctionCapability.ATAN);
        cap.supportScalarFunction(ScalarFunctionCapability.ATAN2);
        cap.supportScalarFunction(ScalarFunctionCapability.CEIL);
        cap.supportScalarFunction(ScalarFunctionCapability.COS);
        cap.supportScalarFunction(ScalarFunctionCapability.COSH);
        cap.supportScalarFunction(ScalarFunctionCapability.COT);
        cap.supportScalarFunction(ScalarFunctionCapability.DEGREES);
        cap.supportScalarFunction(ScalarFunctionCapability.DIV);
        cap.supportScalarFunction(ScalarFunctionCapability.EXP);
        cap.supportScalarFunction(ScalarFunctionCapability.FLOOR);
        cap.supportScalarFunction(ScalarFunctionCapability.GREATEST);
        cap.supportScalarFunction(ScalarFunctionCapability.LEAST);
        cap.supportScalarFunction(ScalarFunctionCapability.LN);
        cap.supportScalarFunction(ScalarFunctionCapability.LOG);
        cap.supportScalarFunction(ScalarFunctionCapability.MOD);
        cap.supportScalarFunction(ScalarFunctionCapability.POWER);
        cap.supportScalarFunction(ScalarFunctionCapability.RADIANS);
        cap.supportScalarFunction(ScalarFunctionCapability.RAND);
        cap.supportScalarFunction(ScalarFunctionCapability.ROUND);
        cap.supportScalarFunction(ScalarFunctionCapability.SIGN);
        cap.supportScalarFunction(ScalarFunctionCapability.SIN);
        cap.supportScalarFunction(ScalarFunctionCapability.SINH);
        cap.supportScalarFunction(ScalarFunctionCapability.SQRT);
        cap.supportScalarFunction(ScalarFunctionCapability.TAN);
        cap.supportScalarFunction(ScalarFunctionCapability.TANH);
        cap.supportScalarFunction(ScalarFunctionCapability.TRUNC);

        cap.supportScalarFunction(ScalarFunctionCapability.ASCII);
        cap.supportScalarFunction(ScalarFunctionCapability.BIT_LENGTH);
        cap.supportScalarFunction(ScalarFunctionCapability.CHR);
        cap.supportScalarFunction(ScalarFunctionCapability.CONCAT);
        cap.supportScalarFunction(ScalarFunctionCapability.INSTR);
        cap.supportScalarFunction(ScalarFunctionCapability.LENGTH);
        cap.supportScalarFunction(ScalarFunctionCapability.LOWER);
        cap.supportScalarFunction(ScalarFunctionCapability.LPAD);
        cap.supportScalarFunction(ScalarFunctionCapability.LTRIM);
        cap.supportScalarFunction(ScalarFunctionCapability.OCTET_LENGTH);
        cap.supportScalarFunction(ScalarFunctionCapability.REGEXP_REPLACE);
        cap.supportScalarFunction(ScalarFunctionCapability.REPEAT);
        cap.supportScalarFunction(ScalarFunctionCapability.REPLACE);
        cap.supportScalarFunction(ScalarFunctionCapability.REVERSE);
        cap.supportScalarFunction(ScalarFunctionCapability.RIGHT);
        cap.supportScalarFunction(ScalarFunctionCapability.RPAD);
        cap.supportScalarFunction(ScalarFunctionCapability.RTRIM);
        cap.supportScalarFunction(ScalarFunctionCapability.SUBSTR);
        cap.supportScalarFunction(ScalarFunctionCapability.TRANSLATE);
        cap.supportScalarFunction(ScalarFunctionCapability.TRIM);
        cap.supportScalarFunction(ScalarFunctionCapability.UNICODE);
        cap.supportScalarFunction(ScalarFunctionCapability.UNICODECHR);
        cap.supportScalarFunction(ScalarFunctionCapability.UPPER);

        cap.supportScalarFunction(ScalarFunctionCapability.ADD_DAYS);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_HOURS);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_MINUTES);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_MONTHS);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_SECONDS);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_WEEKS);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_YEARS);

        cap.supportScalarFunction(ScalarFunctionCapability.SECONDS_BETWEEN);
        cap.supportScalarFunction(ScalarFunctionCapability.MINUTES_BETWEEN);
        cap.supportScalarFunction(ScalarFunctionCapability.HOURS_BETWEEN);
        cap.supportScalarFunction(ScalarFunctionCapability.DAYS_BETWEEN);
        cap.supportScalarFunction(ScalarFunctionCapability.MONTHS_BETWEEN);
        cap.supportScalarFunction(ScalarFunctionCapability.YEARS_BETWEEN);

        cap.supportScalarFunction(ScalarFunctionCapability.MINUTE);
        cap.supportScalarFunction(ScalarFunctionCapability.SECOND);
        cap.supportScalarFunction(ScalarFunctionCapability.DAY);
        cap.supportScalarFunction(ScalarFunctionCapability.WEEK);
        cap.supportScalarFunction(ScalarFunctionCapability.MONTH);
        cap.supportScalarFunction(ScalarFunctionCapability.YEAR);

        cap.supportScalarFunction(ScalarFunctionCapability.CURRENT_DATE);
        cap.supportScalarFunction(ScalarFunctionCapability.CURRENT_TIMESTAMP);
        cap.supportScalarFunction(ScalarFunctionCapability.DATE_TRUNC);

        cap.supportScalarFunction(ScalarFunctionCapability.EXTRACT);
        cap.supportScalarFunction(ScalarFunctionCapability.LOCALTIMESTAMP);
        cap.supportScalarFunction(ScalarFunctionCapability.POSIX_TIME);

        cap.supportScalarFunction(ScalarFunctionCapability.TO_CHAR);

        cap.supportScalarFunction(ScalarFunctionCapability.CASE);

        cap.supportScalarFunction(ScalarFunctionCapability.HASH_MD5);
        return cap;
    }

    @Override
    public DataType dialectSpecificMapJdbcType(final JdbcTypeDescription jdbcTypeDescription) throws SQLException {
        DataType colType = null;
        final int jdbcType = jdbcTypeDescription.getJdbcType();
        switch (jdbcType) {
            case Types.OTHER:
                final String columnTypeName = jdbcTypeDescription.getTypeName();
                if (columnTypeName.equals("varbit")) {
                    final int n = jdbcTypeDescription.getPrecisionOrSize();
                    colType = DataType.createVarChar(n, DataType.ExaCharset.UTF8);
                } else {
                    colType = DataType
                            .createVarChar(PostgreSQLSqlDialect.MAX_POSTGRES_SQL_VARCHAR_SIZE_BASED_ON_EXASOL_LIMIT,
                                    DataType.ExaCharset.UTF8);
                }
                break;
            case Types.SQLXML:
            case Types.DISTINCT:
                colType = DataType.createVarChar(PostgreSQLSqlDialect.MAX_POSTGRES_SQL_VARCHAR_SIZE_BASED_ON_EXASOL_LIMIT,
                        DataType.ExaCharset.UTF8);
                break;
            default:
                break;
        }

        return colType;
    }

    @Override
    public MappedTable mapTable(final ResultSet tables, final List<String> ignoreErrorList) throws SQLException {
        final String tableName = tables.getString("TABLE_NAME");
        if (getContext().getPostgreSQLIdentifierMapping() == PostgreSQLIdentifierMapping.CONVERT_TO_UPPER &&
                !ignoreErrorList.contains(POSTGRES_IGNORE_UPPERCASE_TABLES) &&
                containsUppercaseCharacter(tableName)) {
            throw new IllegalArgumentException("Table " + tableName + " cannot be used in virtual schema. " +
                    "Set property IGNORE_ERRORS to POSTGRESQL_UPPERCASE_TABLES to enforce schema creation.");
        } else {
            return super.mapTable(tables, ignoreErrorList);
        }
    }

    private boolean containsUppercaseCharacter(final String tableName) {
        return !tableName.equals(tableName.toLowerCase());
    }

    @Override
    public Map<ScalarFunction, String> getScalarFunctionAliases() {

        final Map<ScalarFunction, String> scalarAliases = new EnumMap<>(ScalarFunction.class);

        scalarAliases.put(ScalarFunction.SUBSTR, "SUBSTRING");
        scalarAliases.put(ScalarFunction.HASH_MD5, "MD5");

        return scalarAliases;

    }

    @Override
    public SchemaOrCatalogSupport supportsJdbcCatalogs() {
        return SchemaOrCatalogSupport.SUPPORTED;
    }

    @Override
    public SchemaOrCatalogSupport supportsJdbcSchemas() {
        return SchemaOrCatalogSupport.SUPPORTED;
    }

    @Override
    public String changeIdentifierCaseIfNeeded(final String identifier) {
        if (getContext().getPostgreSQLIdentifierMapping() != PostgreSQLIdentifierMapping.PRESERVE_ORIGINAL_CASE) {
            final boolean isSimplePostgresIdentifier = identifier.matches("^[a-z][0-9a-z_]*");

            if (isSimplePostgresIdentifier) {
                return identifier.toUpperCase();
            }
        }
        return identifier;
    }

    @Override
    public IdentifierCaseHandling getUnquotedIdentifierHandling() {
        return IdentifierCaseHandling.INTERPRET_AS_LOWER;
    }

    @Override
    public IdentifierCaseHandling getQuotedIdentifierHandling() {
        return IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE;
    }

    @Override
    public String applyQuote(final String identifier) {
        String postgreSQLIdentifier = identifier;
        if (getContext().getPostgreSQLIdentifierMapping() != PostgreSQLIdentifierMapping.PRESERVE_ORIGINAL_CASE) {
            postgreSQLIdentifier = convertIdentifierToLowerCase(postgreSQLIdentifier);
        }
        return "\"" + postgreSQLIdentifier.replace("\"", "\"\"") + "\"";
    }

    private String convertIdentifierToLowerCase(final String identifier) {
        return identifier.toLowerCase();
    }

    @Override
    public String applyQuoteIfNeeded(final String identifier) {
        // This is a simplified rule, which quotes all identifiers although not needed
        return applyQuote(identifier);
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
        return NullSorting.NULLS_SORTED_AT_END;
    }

    @Override
    public String getStringLiteral(final String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    @Override
    public SqlGenerationVisitor getSqlGenerationVisitor(final SqlGenerationContext context) {
        return new PostgresSQLSqlGenerationVisitor(this, context);
    }

}
