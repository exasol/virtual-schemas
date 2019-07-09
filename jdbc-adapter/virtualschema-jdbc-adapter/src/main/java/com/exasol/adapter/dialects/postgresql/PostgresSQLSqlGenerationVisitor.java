package com.exasol.adapter.dialects.postgresql;

import java.util.*;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.ColumnAdapterNotes;
import com.exasol.adapter.sql.*;
import com.google.common.collect.ImmutableList;

/**
 * This class generates SQL queries for the {@link PostgreSQLSqlDialect}.
 */
public class PostgresSQLSqlGenerationVisitor extends AbstractSqlGenerationVisitor {
    private static final List<String> TYPE_NAMES_REQUIRING_CAST = ImmutableList.of("varbit", "point", "line", "lseg",
            "box", "path", "polygon", "circle", "cidr", "citext", "inet", "macaddr", "interval", "json", "jsonb",
            "uuid", "tsquery", "tsvector", "xml", "smallserial", "serial", "bigserial");
    private static final List<String> TYPE_NAMES_NOT_SUPPORTED = ImmutableList.of("bytea");

    /**
     * Create a new instance of the {@link PostgresSQLSqlGenerationVisitor}.
     *
     * @param dialect {@link PostgreSQLSqlDialect} SQL dialect
     * @param context SQL generation context
     */
    public PostgresSQLSqlGenerationVisitor(final SqlDialect dialect, final SqlGenerationContext context) {
        super(dialect, context);
    }

    @Override
    protected List<String> getListOfTypeNamesRequiringCast() {
        return TYPE_NAMES_REQUIRING_CAST;
    }

    @Override
    protected List<String> getListOfTypeNamesNotSupported() {
        return TYPE_NAMES_NOT_SUPPORTED;
    }

    @Override
    public String visit(final SqlFunctionScalar function) throws AdapterException {
        String sql = super.visit(function);
        final List<String> argumentsSql = new ArrayList<>();
        for (final SqlNode node : function.getArguments()) {
            argumentsSql.add(node.accept(this));
        }
        final ScalarFunction scalarFunction = function.getFunction();
        switch (scalarFunction) {
        case ADD_DAYS:
        case ADD_HOURS:
        case ADD_MINUTES:
        case ADD_SECONDS:
        case ADD_WEEKS:
        case ADD_YEARS:
            sql = getAddDateTime(argumentsSql, scalarFunction);
            break;
        case SECONDS_BETWEEN:
        case MINUTES_BETWEEN:
        case HOURS_BETWEEN:
        case DAYS_BETWEEN:
        case MONTHS_BETWEEN:
        case YEARS_BETWEEN:
            sql = getDateTimeBetween(argumentsSql, scalarFunction);
            break;
        case SECOND:
        case MINUTE:
        case DAY:
        case WEEK:
        case MONTH:
        case YEAR:
            sql = getDateTime(argumentsSql, scalarFunction);
            break;
        case POSIX_TIME:
            sql = getPosixTime(argumentsSql);
            break;
        default:
            break;
        }
        return sql;
    }

    private String getAddDateTime(final List<String> argumentsSql, final ScalarFunction scalarFunction) {
        final StringBuilder builder = new StringBuilder();
        builder.append(argumentsSql.get(0));
        builder.append(" + ");
        switch (scalarFunction) {
        case ADD_DAYS:
            appendInterval(argumentsSql, builder, " day'");
            break;
        case ADD_HOURS:
            appendInterval(argumentsSql, builder, " hour'");
            break;
        case ADD_MINUTES:
            appendInterval(argumentsSql, builder, " minute'");
            break;
        case ADD_SECONDS:
            appendInterval(argumentsSql, builder, " second'");
            break;
        case ADD_WEEKS:
            appendInterval(argumentsSql, builder, " week'");
            break;
        case ADD_YEARS:
            appendInterval(argumentsSql, builder, " year'");
            break;
        default:
            break;
        }
        return builder.toString();
    }

    private void appendInterval(final List<String> argumentsSql, final StringBuilder builder,
            final String stringToAppend) {
        builder.append(" interval '");
        builder.append(argumentsSql.get(1));
        builder.append(stringToAppend);
    }

    private String getDateTimeBetween(final List<String> argumentsSql, final ScalarFunction scalarFunction) {
        final StringBuilder builder = new StringBuilder();
        builder.append("DATE_PART(");
        switch (scalarFunction) {
        case SECONDS_BETWEEN:
            builder.append("'SECOND'");
            break;
        case MINUTES_BETWEEN:
            builder.append("'MINUTE'");
            break;
        case HOURS_BETWEEN:
            builder.append("'HOUR'");
            break;
        case DAYS_BETWEEN:
            builder.append("'DAY'");
            break;
        case MONTHS_BETWEEN:
            builder.append("'MONTH'");
            break;
        case YEARS_BETWEEN:
            builder.append("'YEAR'");
            break;
        default:
            break;
        }
        builder.append(", AGE(");
        builder.append(argumentsSql.get(1));
        builder.append(",");
        builder.append(argumentsSql.get(0));
        builder.append("))");
        return builder.toString();
    }

    private String getDateTime(final List<String> argumentsSql, final ScalarFunction scalarFunction) {
        final StringBuilder builder = new StringBuilder();
        builder.append("DATE_PART(");
        switch (scalarFunction) {
        case SECOND:
            builder.append("'SECOND'");
            break;
        case MINUTE:
            builder.append("'MINUTE'");
            break;
        case DAY:
            builder.append("'DAY'");
            break;
        case WEEK:
            builder.append("'WEEK'");
            break;
        case MONTH:
            builder.append("'MONTH'");
            break;
        case YEAR:
            builder.append("'YEAR'");
            break;
        default:
            break;
        }
        builder.append(",");
        builder.append(argumentsSql.get(0));
        builder.append(")");
        return builder.toString();
    }

    private String getPosixTime(final List<String> argumentsSql) {
        return "EXTRACT(EPOCH FROM " + argumentsSql.get(0) + ")";
    }

    @Override
    public String visit(final SqlColumn column) throws AdapterException {
        return getColumnProjectionString(column, super.visit(column));
    }

    private String getColumnProjectionString(final SqlColumn column, final String projectionString)
            throws AdapterException {
        final boolean isDirectlyInSelectList = (column.hasParent()
                && column.getParent().getType() == SqlNodeType.SELECT_LIST);
        if (!isDirectlyInSelectList) {
            return projectionString;
        }
        final String typeName = ColumnAdapterNotes
                .deserialize(column.getMetadata().getAdapterNotes(), column.getMetadata().getName()).getTypeName();
        return buildColumnProjectionString(typeName, projectionString);
    }

    @Override
    protected String buildColumnProjectionString(final String typeName, String projectionString) {
        if (checkIfNeedToCastToVarchar(typeName)) {
            projectionString = "CAST(" + projectionString + "  as VARCHAR )";
        } else if (typeName.startsWith("smallserial")) {
            projectionString = "CAST(" + projectionString + "  as SMALLINT )";
        } else if (typeName.startsWith("serial")) {
            projectionString = "CAST(" + projectionString + "  as INTEGER )";
        } else if (typeName.startsWith("bigserial")) {
            projectionString = "CAST(" + projectionString + "  as BIGINT )";
        } else if (TYPE_NAMES_NOT_SUPPORTED.contains(typeName)) {
            projectionString = "cast('" + typeName + " NOT SUPPORTED' as varchar) as not_supported";
        }
        return projectionString;
    }

    private boolean checkIfNeedToCastToVarchar(final String typeName) {
        final List<String> typesToVarcharCast = Arrays.asList("point", "line", "varbit", "lseg", "box", "path",
                "polygon", "circle", "cidr", "citext", "inet", "macaddr", "interval", "json", "jsonb", "uuid",
                "tsquery", "tsvector", "xml");
        return typesToVarcharCast.contains(typeName);
    }

    @Override
    public String visit(final SqlFunctionAggregateGroupConcat function) throws AdapterException {
        final StringBuilder builder = new StringBuilder();
        builder.append("STRING_AGG");
        builder.append("(");
        if (function.getArguments() != null && function.getArguments().size() == 1
                && function.getArguments().get(0) != null) {
            final String expression = function.getArguments().get(0).accept(this);
            builder.append(expression);
            builder.append(", ");
            String separator = ",";
            if (function.getSeparator() != null) {
                separator = function.getSeparator();
            }
            builder.append("'");
            builder.append(separator);
            builder.append("') ");
            return builder.toString();
        } else {
            throw new SqlGenerationVisitorException(
                    "List of arguments of SqlFunctionAggregateGroupConcat should have one argument.");
        }
    }
}