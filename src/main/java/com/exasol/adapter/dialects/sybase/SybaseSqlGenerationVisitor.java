package com.exasol.adapter.dialects.sybase;

import static com.exasol.adapter.dialects.sybase.SybaseSqlDialect.MAX_SYBASE_N_VARCHAR_SIZE;
import static com.exasol.adapter.dialects.sybase.SybaseSqlDialect.MAX_SYBASE_VARCHAR_SIZE;

import java.util.*;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.sql.*;

/**
 * This class generates SQL queries for the {@link SybaseSqlGenerationVisitor}.
 */
public class SybaseSqlGenerationVisitor extends SqlGenerationVisitor {
    private static final List<String> TYPE_NAMES_REQUIRING_CAST = List.of("text", "time", "bigtime", "xml");
    private static final List<String> TYPE_NAME_NOT_SUPPORTED = List.of("varbinary", "binary", "image");

    /**
     * Create a new instance of the {@link SybaseSqlGenerationVisitor}.
     *
     * @param dialect {@link SybaseSqlDialect} SQL dialect
     * @param context SQL generation context
     */
    public SybaseSqlGenerationVisitor(final SqlDialect dialect, final SqlGenerationContext context) {
        super(dialect, context);
    }

    protected List<String> getListOfTypeNamesRequiringCast() {
        return TYPE_NAMES_REQUIRING_CAST;
    }

    protected List<String> getListOfTypeNamesNotSupported() {
        return TYPE_NAME_NOT_SUPPORTED;
    }

    protected String buildColumnProjectionString(final String typeName, final String projectionString) {
        if (typeName.startsWith("text")) {
            return getCastAs(projectionString, "NVARCHAR(" + MAX_SYBASE_N_VARCHAR_SIZE + ")");
        } else if (typeName.equals("time")) {
            return "CONVERT(VARCHAR(12), " + projectionString + ", 137)";
        } else if (typeName.equals("bigtime")) {
            return "CONVERT(VARCHAR(16), " + projectionString + ", 137)";
        } else if (typeName.startsWith("xml")) {
            return getCastAs(projectionString, "NVARCHAR(" + MAX_SYBASE_N_VARCHAR_SIZE + ")");
        } else if (TYPE_NAME_NOT_SUPPORTED.contains(typeName)) {
            return "'" + typeName + " NOT SUPPORTED'";
        } else {
            return projectionString;
        }
    }

    private String getCastAs(final String projectionString, final String castType) {
        return "CAST(" + projectionString + "  as " + castType + " )";
    }

    @Override
    protected String representAsteriskInSelectList(final SqlSelectList selectList) throws AdapterException {
        final List<String> selectStarList = buildSelectStar(selectList);
        final List<String> selectListElements = new ArrayList<>(selectStarList.size());
        selectListElements.addAll(selectStarList);
        return String.join(", ", selectListElements);
    }

    private List<String> buildSelectStar(final SqlSelectList selectList) throws AdapterException {
        if (SqlGenerationHelper.selectListRequiresCasts(selectList, this.nodeRequiresCast)) {
            return buildSelectStarWithNodeCast(selectList);
        } else {
            return new ArrayList<>(Collections.singletonList("*"));
        }
    }

    private List<String> buildSelectStarWithNodeCast(final SqlSelectList selectList) throws AdapterException {
        final SqlStatementSelect select = (SqlStatementSelect) selectList.getParent();
        int columnId = 0;
        final List<TableMetadata> tableMetadata = new ArrayList<>();
        SqlGenerationHelper.addMetadata(select.getFromClause(), tableMetadata);
        final List<String> selectListElements = new ArrayList<>(tableMetadata.size());
        for (final TableMetadata tableMeta : tableMetadata) {
            for (final ColumnMetadata columnMeta : tableMeta.getColumns()) {
                final SqlColumn sqlColumn = new SqlColumn(columnId, columnMeta);
                selectListElements.add(buildColumnProjectionString(sqlColumn, super.visit(sqlColumn)));
                ++columnId;
            }
        }
        return selectListElements;
    }

    private String buildColumnProjectionString(final SqlColumn column, final String projectionString)
            throws AdapterException {
        return buildColumnProjectionString(getTypeNameFromColumn(column), projectionString);
    }

    private final java.util.function.Predicate<SqlNode> nodeRequiresCast = node -> {
        try {
            if (node.getType() == SqlNodeType.COLUMN) {
                final SqlColumn column = (SqlColumn) node;
                final String typeName = getTypeNameFromColumn(column);
                return getListOfTypeNamesRequiringCast().contains(typeName)
                        || getListOfTypeNamesNotSupported().contains(typeName);
            }
            return false;
        } catch (final AdapterException exception) {
            throw new SqlGenerationVisitorException("Exception during deserialization of ColumnAdapterNotes. ",
                    exception);
        }
    };

    @Override
    public String visit(final SqlColumn column) throws AdapterException {
        final String projectionString = super.visit(column);
        return getColumnProjectionString(column, projectionString);
    }

    private String getColumnProjectionString(final SqlColumn column, final String projectionString)
            throws AdapterException {
        return super.isDirectlyInSelectList(column) //
                ? buildColumnProjectionString(getTypeNameFromColumn(column), projectionString) //
                : projectionString;
    }

    @Override
    public String visit(final SqlOrderBy orderBy) throws AdapterException {
        final int size = orderBy.getExpressions().size();
        final List<String> sqlOrderElement = new ArrayList<>(size);
        for (int i = 0; i < size; ++i) {
            String elementSql = orderBy.getExpressions().get(i).accept(this);
            final boolean isNullsLast = orderBy.nullsLast().get(i);
            final boolean isAscending = orderBy.isAscending().get(i);
            if (!isAscending && !isNullsLast) {
                elementSql = "(CASE WHEN " + elementSql + " IS NULL THEN 0 ELSE 1 END), " + elementSql;
            }
            if (isAscending && isNullsLast) {
                elementSql = "(CASE WHEN " + elementSql + " IS NULL THEN 1 ELSE 0 END), " + elementSql;
            }
            if (!isAscending) {
                elementSql += " DESC";
            }
            sqlOrderElement.add(elementSql);
        }
        return "ORDER BY " + String.join(" ,", sqlOrderElement);
    }

    @Override
    public String visit(final SqlStatementSelect select) throws AdapterException {
        if (!select.hasLimit()) {
            return super.visit(select);
        } else {
            final SqlLimit limit = select.getLimit();
            final StringBuilder builder = new StringBuilder();
            builder.append("SELECT TOP " + limit.getLimit() + " ");
            builder.append(select.getSelectList().accept(this));
            builder.append(" FROM ");
            builder.append(select.getFromClause().accept(this));
            if (select.hasFilter()) {
                builder.append(" WHERE ");
                builder.append(select.getWhereClause().accept(this));
            }
            if (select.hasGroupBy()) {
                builder.append(" GROUP BY ");
                builder.append(select.getGroupBy().accept(this));
            }
            if (select.hasHaving()) {
                builder.append(" HAVING ");
                builder.append(select.getHaving().accept(this));
            }
            if (select.hasOrderBy()) {
                builder.append(" ");
                builder.append(select.getOrderBy().accept(this));
            }
            return builder.toString();
        }
    }

    @Override
    public String visit(final SqlFunctionScalar function) throws AdapterException {
        final List<SqlNode> arguments = function.getArguments();
        final List<String> argumentsSql = new ArrayList<>(arguments.size());
        for (final SqlNode node : arguments) {
            argumentsSql.add(node.accept(this));
        }
        switch (function.getFunction()) {
        case INSTR:
            return getInstr(argumentsSql);
        case LPAD:
            return getLdap(argumentsSql);
        case RPAD:
            return getRpad(argumentsSql);
        case ADD_DAYS:
        case ADD_HOURS:
        case ADD_MINUTES:
        case ADD_SECONDS:
        case ADD_WEEKS:
        case ADD_YEARS:
            return getAddDateTime(function, argumentsSql);
        case SECONDS_BETWEEN:
        case MINUTES_BETWEEN:
        case HOURS_BETWEEN:
        case DAYS_BETWEEN:
        case MONTHS_BETWEEN:
        case YEARS_BETWEEN:
            return getDateTimeBetween(function, argumentsSql);
        case CURRENT_DATE:
            return "CAST( GETDATE() AS DATE)";
        case CURRENT_TIMESTAMP:
            return "GETDATE()";
        case SYSDATE:
            return "CAST( SYSDATETIME() AS DATE)";
        case SYSTIMESTAMP:
            return "SYSDATETIME()";
        case ST_X:
            return argumentsSql.get(0) + ".STX";
        case ST_Y:
            return argumentsSql.get(0) + ".STY";
        case ST_ENDPOINT:
            return getScalarFunctionWithVarcharCast(argumentsSql, ".STEndPoint()");
        case ST_ISCLOSED:
            return argumentsSql.get(0) + ".STIsClosed()";
        case ST_ISRING:
            return argumentsSql.get(0) + ".STIsRing()";
        case ST_LENGTH:
            return argumentsSql.get(0) + ".STLength()";
        case ST_NUMPOINTS:
            return argumentsSql.get(0) + ".STNumPoints()";
        case ST_POINTN:
            return getScalarFunctionWithVarcharCastTwoArguments(argumentsSql, ".STPointN(");
        case ST_STARTPOINT:
            return getScalarFunctionWithVarcharCast(argumentsSql, ".STStartPoint()");
        case ST_AREA:
            return argumentsSql.get(0) + ".STArea()";
        case ST_EXTERIORRING:
            return getScalarFunctionWithVarcharCast(argumentsSql, ".STExteriorRing()");
        case ST_INTERIORRINGN:
            return getScalarFunctionWithVarcharCastTwoArguments(argumentsSql, ".STInteriorRingN (");
        case ST_NUMINTERIORRINGS:
            return argumentsSql.get(0) + ".STNumInteriorRing()";
        case ST_GEOMETRYN:
            return getScalarFunctionWithVarcharCastTwoArguments(argumentsSql, ".STGeometryN(");
        case ST_NUMGEOMETRIES:
            return argumentsSql.get(0) + ".STNumGeometries()";
        case ST_BOUNDARY:
            return getScalarFunctionWithVarcharCast(argumentsSql, ".STBoundary()");
        case ST_BUFFER:
            return getScalarFunctionWithVarcharCastTwoArguments(argumentsSql, ".STBuffer(");
        case ST_CENTROID:
            return getScalarFunctionWithVarcharCast(argumentsSql, ".STCentroid()");
        case ST_CONTAINS:
            return argumentsSql.get(0) + ".STContains(" + argumentsSql.get(1) + ")";
        case ST_CONVEXHULL:
            return getScalarFunctionWithVarcharCast(argumentsSql, ".STConvexHull()");
        case ST_CROSSES:
            return argumentsSql.get(0) + ".STCrosses(" + argumentsSql.get(1) + ")";
        case ST_DIFFERENCE:
            return getScalarFunctionWithVarcharCastTwoArguments(argumentsSql, ".STDifference(");
        case ST_DIMENSION:
            return argumentsSql.get(0) + ".STDimension()";
        case ST_DISJOINT:
            return getScalarFunctionWithVarcharCastTwoArguments(argumentsSql, ".STDisjoint(");
        case ST_DISTANCE:
            return argumentsSql.get(0) + ".STDistance(" + argumentsSql.get(1) + ")";
        case ST_ENVELOPE:
            return getScalarFunctionWithVarcharCast(argumentsSql, ".STEnvelope()");
        case ST_EQUALS:
            return argumentsSql.get(0) + ".STEquals(" + argumentsSql.get(1) + ")";
        case ST_GEOMETRYTYPE:
            return argumentsSql.get(0) + ".STGeometryType()";
        case ST_INTERSECTION:
            return getScalarFunctionWithVarcharCastTwoArguments(argumentsSql, ".STIntersection(");
        case ST_INTERSECTS:
            return argumentsSql.get(0) + ".STIntersects(" + argumentsSql.get(1) + ")";
        case ST_ISEMPTY:
            return argumentsSql.get(0) + ".STIsEmpty()";
        case ST_ISSIMPLE:
            return argumentsSql.get(0) + ".STIsSimple()";
        case ST_OVERLAPS:
            return argumentsSql.get(0) + ".STOverlaps(" + argumentsSql.get(1) + ")";
        case ST_SYMDIFFERENCE:
            return getScalarFunctionWithVarcharCastTwoArguments(argumentsSql, ".STSymDifference (");
        case ST_TOUCHES:
            return argumentsSql.get(0) + ".STTouches(" + argumentsSql.get(1) + ")";
        case ST_UNION:
            return getScalarFunctionWithVarcharCastTwoArguments(argumentsSql, ".STUnion(");
        case ST_WITHIN:
            return argumentsSql.get(0) + ".STWithin(" + argumentsSql.get(1) + ")";
        case BIT_AND:
            return argumentsSql.get(0) + " & " + argumentsSql.get(1);
        case BIT_OR:
            return argumentsSql.get(0) + " | " + argumentsSql.get(1);
        case BIT_XOR:
            return argumentsSql.get(0) + " ^ " + argumentsSql.get(1);
        case BIT_NOT:
            return "~ " + argumentsSql.get(0);
        case HASH_MD5:
            return "CONVERT(Char, HASHBYTES('MD5'," + argumentsSql.get(0) + "), 2)";
        case HASH_SHA1:
            return "CONVERT(Char, HASHBYTES('SHA1'," + argumentsSql.get(0) + "), 2)";
        case ZEROIFNULL:
            return "ISNULL(" + argumentsSql.get(0) + ",0)";
        default:
            return super.visit(function);
        }
    }

    private String getInstr(final List<String> argumentsSql) {
        final StringBuilder builder = new StringBuilder();
        builder.append("CHARINDEX(");
        builder.append(argumentsSql.get(1));
        builder.append(", ");
        builder.append(argumentsSql.get(0));
        if (argumentsSql.size() > 2) {
            builder.append(", ");
            builder.append(argumentsSql.get(2));
        }
        builder.append(")");
        return builder.toString();
    }

    private String getLdap(final List<String> argumentsSql) {
        final StringBuilder builder = new StringBuilder();
        String padChar = "' '";
        if (argumentsSql.size() > 2) {
            padChar = argumentsSql.get(2);
        }
        final String string = argumentsSql.get(0);
        final String length = argumentsSql.get(1);
        builder.append("RIGHT ( REPLICATE(");
        builder.append(padChar);
        builder.append(",");
        builder.append(length);
        builder.append(") + LEFT(");
        builder.append(string);
        builder.append(",");
        builder.append(length);
        builder.append("),");
        builder.append(length);
        builder.append(")");
        return builder.toString();
    }

    private String getRpad(final List<String> argumentsSql) {
        final StringBuilder builder = new StringBuilder();
        String padChar = "' '";
        if (argumentsSql.size() > 2) {
            padChar = argumentsSql.get(2);
        }
        final String string = argumentsSql.get(0);
        final String length = argumentsSql.get(1);
        builder.append("LEFT(RIGHT(");
        builder.append(string);
        builder.append(",");
        builder.append(length);
        builder.append(") + REPLICATE(");
        builder.append(padChar);
        builder.append(",");
        builder.append(length);
        builder.append("),");
        builder.append(length);
        builder.append(")");
        return builder.toString();
    }

    private String getAddDateTime(final SqlFunctionScalar function, final List<String> argumentsSql) {
        final StringBuilder builder = new StringBuilder();
        builder.append("DATEADD(");
        switch (function.getFunction()) {
        case ADD_DAYS:
            builder.append("DAY");
            break;
        case ADD_HOURS:
            builder.append("HOUR");
            break;
        case ADD_MINUTES:
            builder.append("MINUTE");
            break;
        case ADD_SECONDS:
            builder.append("SECOND");
            break;
        case ADD_WEEKS:
            builder.append("WEEK");
            break;
        case ADD_YEARS:
            builder.append("YEAR");
            break;
        default:
            break;
        }
        builder.append(",");
        builder.append(argumentsSql.get(1));
        builder.append(",");
        builder.append(argumentsSql.get(0));
        builder.append(")");
        return builder.toString();
    }

    private String getDateTimeBetween(final SqlFunctionScalar function, final List<String> argumentsSql) {
        final StringBuilder builder = new StringBuilder();
        builder.append("DATEDIFF(");
        switch (function.getFunction()) {
        case SECONDS_BETWEEN:
            builder.append("SECOND");
            break;
        case MINUTES_BETWEEN:
            builder.append("MINUTE");
            break;
        case HOURS_BETWEEN:
            builder.append("HOUR");
            break;
        case DAYS_BETWEEN:
            builder.append("DAY");
            break;
        case MONTHS_BETWEEN:
            builder.append("MONTH");
            break;
        case YEARS_BETWEEN:
            builder.append("YEAR");
            break;
        default:
            break;
        }
        builder.append(",");
        builder.append(argumentsSql.get(1));
        builder.append(",");
        builder.append(argumentsSql.get(0));
        builder.append(")");
        return builder.toString();
    }

    private String getScalarFunctionWithVarcharCast(final List<String> argumentsSql, final String function) {
        return "CAST(" + argumentsSql.get(0) + function + "as VARCHAR(" + MAX_SYBASE_VARCHAR_SIZE + ") )";
    }

    private String getScalarFunctionWithVarcharCastTwoArguments(final List<String> argumentsSql,
            final String function) {
        return "CAST(" + (argumentsSql.get(0) + function + argumentsSql.get(1) + ")") + "as VARCHAR("
                + MAX_SYBASE_VARCHAR_SIZE + ") )";
    }
}