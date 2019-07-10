package com.exasol.adapter.dialects.sqlserver;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.sql.*;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static com.exasol.adapter.dialects.sqlserver.SqlServerSqlDialect.*;
import static com.exasol.adapter.dialects.sqlserver.SqlServerSqlDialect.MAX_SQLSERVER_NVARCHAR_SIZE;

/**
 * This class generates SQL queries for the {@link SqlServerSqlDialect}.
 */
public class SqlServerSqlGenerationVisitor extends AbstractSqlGenerationVisitor {
    private static final List<String> TYPE_NAMES_REQUIRING_CAST = ImmutableList.of("text", "date", "datetime2",
            "hierarchyid", "geometry", "geography", "timestamp", "xml");
    private static final List<String> TYPE_NAME_NOT_SUPPORTED = ImmutableList.of("varbinary", "binary");

    /**
     * Create a new instance of the {@link SqlServerSqlGenerationVisitor}.
     *
     * @param dialect {@link SqlServerSqlDialect} SQL dialect
     * @param context SQL generation context
     */
    public SqlServerSqlGenerationVisitor(final SqlDialect dialect, final SqlGenerationContext context) {
        super(dialect, context);
    }

    @Override
    protected List<String> getListOfTypeNamesRequiringCast() {
        return TYPE_NAMES_REQUIRING_CAST;
    }

    @Override
    protected List<String> getListOfTypeNamesNotSupported() {
        return TYPE_NAME_NOT_SUPPORTED;
    }

    @Override
    protected String buildColumnProjectionString(final String typeName, final String projectionString) {
        final String castTypeNVarchar = "NVARCHAR(" + MAX_SQLSERVER_NVARCHAR_SIZE + ")";
        if (typeName.startsWith("text")) {
            return getCastAs(projectionString, castTypeNVarchar);
        } else if (typeName.startsWith("date") || typeName.startsWith("datetime2")
                || typeName.startsWith("timestamp")) {
            return getCastAs(projectionString, "DateTime");
        } else if (typeName.startsWith("hierarchyid")) {
            return getCastAs(projectionString, castTypeNVarchar);
        } else if (typeName.startsWith("geometry") || typeName.startsWith("geography")) {
            return getCastAs(projectionString, "VARCHAR(" + MAX_SQLSERVER_VARCHAR_SIZE + ")");
        } else if (typeName.startsWith("xml")) {
            return getCastAs(projectionString, castTypeNVarchar);
        } else if (TYPE_NAME_NOT_SUPPORTED.contains(typeName)) {
            return getScalarFunctionBit("'", typeName, " NOT SUPPORTED'");
        } else {
            return projectionString;
        }
    }

    private String getCastAs(final String projectionString, final String castType) {
        return "CAST(" + projectionString + "  as " + castType + " )";
    }

    private String getScalarFunctionBit(final String s, final String s2, final String s3) {
        return s + s2 + s3;
    }

    @Override
    public String visit(final SqlSelectList selectList) throws AdapterException {
        if (selectList.isRequestAnyColumn()) {
            return "true";
        } else {
            return super.getSelectList(selectList);
        }
    }

    @Override
    public String visit(final SqlStatementSelect select) throws AdapterException {
        if (!select.hasLimit()) {
            return super.visit(select);
        } else {
            final SqlLimit limit = select.getLimit();
            final StringBuilder builder = new StringBuilder();
            builder.append("SELECT TOP ");
            builder.append(limit.getLimit());
            builder.append(" ");
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
        String sql = super.visit(function);
        final List<String> argumentsSql = new ArrayList<>();
        for (final SqlNode node : function.getArguments()) {
            argumentsSql.add(node.accept(this));
        }
        switch (function.getFunction()) {
        case INSTR:
            sql = getInstr(argumentsSql);
            break;
        case LPAD:
            sql = getLdap(argumentsSql);
            break;
        case RPAD:
            sql = getRpad(argumentsSql);
            break;
        case ADD_DAYS:
        case ADD_HOURS:
        case ADD_MINUTES:
        case ADD_SECONDS:
        case ADD_WEEKS:
        case ADD_YEARS:
            sql = getAddDateTime(function, argumentsSql);
            break;
        case SECONDS_BETWEEN:
        case MINUTES_BETWEEN:
        case HOURS_BETWEEN:
        case DAYS_BETWEEN:
        case MONTHS_BETWEEN:
        case YEARS_BETWEEN:
            sql = getDateTimeBetween(function, argumentsSql);
            break;
        case CURRENT_DATE:
            sql = "CAST( GETDATE() AS DATE)";
            break;
        case CURRENT_TIMESTAMP:
            sql = "GETDATE()";
            break;
        case SYSDATE:
            sql = "CAST( SYSDATETIME() AS DATE)";
            break;
        case SYSTIMESTAMP:
            sql = "SYSDATETIME()";
            break;
        case ST_X:
            sql = argumentsSql.get(0) + ".STX";
            break;
        case ST_Y:
            sql = argumentsSql.get(0) + ".STY";
            break;
        case ST_ENDPOINT:
            sql = getScalarFunctionWithVarcharCast(argumentsSql, ".STEndPoint()");
            break;
        case ST_ISCLOSED:
            sql = argumentsSql.get(0) + ".STIsClosed()";
            break;
        case ST_ISRING:
            sql = argumentsSql.get(0) + ".STIsRing()";
            break;
        case ST_LENGTH:
            sql = argumentsSql.get(0) + ".STLength()";
            break;
        case ST_NUMPOINTS:
            sql = argumentsSql.get(0) + ".STNumPoints()";
            break;
        case ST_POINTN:
            sql = getStPointn(argumentsSql, ".STPointN(");
            break;
        case ST_STARTPOINT:
            sql = getScalarFunctionWithVarcharCast(argumentsSql, ".STStartPoint()");
            break;
        case ST_AREA:
            sql = argumentsSql.get(0) + ".STArea()";
            break;
        case ST_EXTERIORRING:
            sql = getScalarFunctionWithVarcharCast(argumentsSql, ".STExteriorRing()");
            break;
        case ST_INTERIORRINGN:
            sql = getStPointn(argumentsSql, ".STInteriorRingN (");
            break;
        case ST_NUMINTERIORRINGS:
            sql = argumentsSql.get(0) + ".STNumInteriorRing()";
            break;
        case ST_GEOMETRYN:
            sql = getStPointn(argumentsSql, ".STGeometryN(");
            break;
        case ST_NUMGEOMETRIES:
            sql = argumentsSql.get(0) + ".STNumGeometries()";
            break;
        case ST_BOUNDARY:
            sql = getScalarFunctionWithVarcharCast(argumentsSql, ".STBoundary()");
            break;
        case ST_BUFFER:
            sql = getStPointn(argumentsSql, ".STBuffer(");
            break;
        case ST_CENTROID:
            sql = getScalarFunctionWithVarcharCast(argumentsSql, ".STCentroid()");
            break;
        case ST_CONTAINS:
            sql = argumentsSql.get(0) + ".STContains(" + argumentsSql.get(1) + ")";
            break;
        case ST_CONVEXHULL:
            sql = getScalarFunctionWithVarcharCast(argumentsSql, ".STConvexHull()");
            break;
        case ST_CROSSES:
            sql = argumentsSql.get(0) + ".STCrosses(" + argumentsSql.get(1) + ")";
            break;
        case ST_DIFFERENCE:
            sql = getStPointn(argumentsSql, ".STDifference(");
            break;
        case ST_DIMENSION:
            sql = argumentsSql.get(0) + ".STDimension()";
            break;
        case ST_DISJOINT:
            sql = getStPointn(argumentsSql, ".STDisjoint(");
            break;
        case ST_DISTANCE:
            sql = argumentsSql.get(0) + ".STDistance(" + argumentsSql.get(1) + ")";
            break;
        case ST_ENVELOPE:
            sql = getScalarFunctionWithVarcharCast(argumentsSql, ".STEnvelope()");
            break;
        case ST_EQUALS:
            sql = argumentsSql.get(0) + ".STEquals(" + argumentsSql.get(1) + ")";
            break;
        case ST_GEOMETRYTYPE:
            sql = argumentsSql.get(0) + ".STGeometryType()";
            break;
        case ST_INTERSECTION:
            sql = getStPointn(argumentsSql, ".STIntersection(");
            break;
        case ST_INTERSECTS:
            sql = argumentsSql.get(0) + ".STIntersects(" + argumentsSql.get(1) + ")";
            break;
        case ST_ISEMPTY:
            sql = argumentsSql.get(0) + ".STIsEmpty()";
            break;
        case ST_ISSIMPLE:
            sql = argumentsSql.get(0) + ".STIsSimple()";
            break;
        case ST_OVERLAPS:
            sql = argumentsSql.get(0) + ".STOverlaps(" + argumentsSql.get(1) + ")";
            break;
        case ST_SYMDIFFERENCE:
            sql = getStPointn(argumentsSql, ".STSymDifference (");
            break;
        case ST_TOUCHES:
            sql = argumentsSql.get(0) + ".STTouches(" + argumentsSql.get(1) + ")";
            break;
        case ST_UNION:
            sql = getStPointn(argumentsSql, ".STUnion(");
            break;
        case ST_WITHIN:
            sql = argumentsSql.get(0) + ".STWithin(" + argumentsSql.get(1) + ")";
            break;
        case BIT_AND:
            sql = getScalarFunctionBit(argumentsSql.get(0), " & ", argumentsSql.get(1));
            break;
        case BIT_OR:
            sql = getScalarFunctionBit(argumentsSql.get(0), " | ", argumentsSql.get(1));
            break;
        case BIT_XOR:
            sql = getScalarFunctionBit(argumentsSql.get(0), " ^ ", argumentsSql.get(1));
            break;
        case BIT_NOT:
            sql = "~ " + argumentsSql.get(0);
            break;
        case HASH_MD5:
            sql = getScalarFunctionBit("CONVERT(Char, HASHBYTES('MD5',", argumentsSql.get(0), "), 2)");
            break;
        case HASH_SHA1:
            sql = getScalarFunctionBit("CONVERT(Char, HASHBYTES('SHA1',", argumentsSql.get(0), "), 2)");
            break;
        case HASH_SHA:
            sql = getScalarFunctionBit("CONVERT(Char, HASHBYTES('SHA',", argumentsSql.get(0), "), 2)");
            break;
        case ZEROIFNULL:
            sql = getScalarFunctionBit("ISNULL(", argumentsSql.get(0), ",0)");
            break;
        default:
            break;
        }
        return sql;
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
        return "CAST(" + argumentsSql.get(0) + function + "as VARCHAR(" + MAX_SQLSERVER_VARCHAR_SIZE + ") )";
    }

    private String getStPointn(final List<String> argumentsSql, final String s) {
        return "CAST(" + (argumentsSql.get(0) + s + argumentsSql.get(1) + ")") + "as VARCHAR("
                + MAX_SQLSERVER_VARCHAR_SIZE + ") )";
    }
}