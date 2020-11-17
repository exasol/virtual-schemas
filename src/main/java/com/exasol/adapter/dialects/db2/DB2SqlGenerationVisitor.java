package com.exasol.adapter.dialects.db2;

import java.util.*;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.sql.*;

/**
 * This class generates SQL queries for the {@link DB2SqlDialect}.
 */
public class DB2SqlGenerationVisitor extends SqlGenerationVisitor {
    private static final List<String> TYPE_NAMES_REQUIRING_CAST = List.of("TIMESTAMP", "DECFLOAT", "CLOB", "XML",
            "TIME");
    private static final List<String> TYPE_NAMES_NOT_SUPPORTED = List.of("BLOB");

    /**
     * Create a new instance of the {@link DB2SqlGenerationVisitor}.
     *
     * @param dialect {@link DB2SqlDialect} SQL dialect
     * @param context SQL generation context
     */
    public DB2SqlGenerationVisitor(final SqlDialect dialect, final SqlGenerationContext context) {
        super(dialect, context);
    }

    protected List<String> getListOfTypeNamesRequiringCast() {
        return TYPE_NAMES_REQUIRING_CAST;
    }

    protected List<String> getListOfTypeNamesNotSupported() {
        return TYPE_NAMES_NOT_SUPPORTED;
    }

    @Override
    protected String representAnyColumnInSelectList() {
        return SqlConstants.ONE;
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
        final String typeName = getTypeNameFromColumn(column);
        return buildColumnProjectionString(typeName, projectionString);
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
        if (!super.isDirectlyInSelectList(column)) {
            return projectionString;
        } else {
            final String typeName = getTypeNameFromColumn(column);
            return buildColumnProjectionString(typeName, projectionString);
        }
    }

    private String buildColumnProjectionString(final String typeName, final String projectionString) {
        if (TYPE_NAMES_NOT_SUPPORTED.contains(typeName)) {
            return "'" + typeName + " NOT SUPPORTED'";
        } else {
            return getProjectionStringWithSupportedTypes(typeName, projectionString);
        }
    }

    private String getProjectionStringWithSupportedTypes(final String typeName, final String projectionString) {
        switch (typeName) {
        case "XML":
            return "XMLSERIALIZE(" + projectionString + " as VARCHAR(32000) INCLUDING XMLDECLARATION)";
        // db2 does not support cast of clobs to varchar in full length -> max 32672
        case "CLOB":
            return "CAST(SUBSTRING(" + projectionString + ",32672) AS VARCHAR(32672))";
        case "CHAR () FOR BIT DATA":
        case "VARCHAR () FOR BIT DATA":
            return "HEX(" + projectionString + ")";
        case "TIME":
            // cast timestamp to not lose precision
        case "TIMESTAMP":
            return "VARCHAR(" + projectionString + ")";
        default:
            return projectionString;
        }
    }

    @Override
    public String visit(final SqlStatementSelect select) throws AdapterException {
        if (!select.hasLimit()) {
            return super.visit(select);
        } else {
            return getSelect(select);
        }
    }

    private String getSelect(final SqlStatementSelect select) throws AdapterException {
        final SqlLimit limit = select.getLimit();
        final StringBuilder builder = new StringBuilder();
        builder.append("SELECT ");
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
        builder.append(" FETCH FIRST ");
        builder.append(limit.getLimit());
        builder.append(" ROWS ONLY");
        return builder.toString();
    }

    @Override
    public String visit(final SqlFunctionScalar function) throws AdapterException {
        switch (function.getFunction()) {
        case TRIM:
            return getTrim(function);
        case ADD_DAYS:
        case ADD_HOURS:
        case ADD_MINUTES:
        case ADD_SECONDS:
        case ADD_WEEKS:
        case ADD_YEARS:
            return getAddTimeOrDate(function);
        case CURRENT_DATE:
        case SYSDATE:
            return "CURRENT DATE";
        case CURRENT_TIMESTAMP:
        case SYSTIMESTAMP:
            return "VARCHAR(CURRENT TIMESTAMP)";
        case DBTIMEZONE:
            return "DBTIMEZONE";
        case LOCALTIMESTAMP:
            return "LOCALTIMESTAMP";
        case SESSIONTIMEZONE:
            return "SESSIONTIMEZONE";
        case BIT_AND:
            return super.visit(function).replaceFirst("^BIT_AND", "BITAND");
        case BIT_TO_NUM:
            return super.visit(function).replaceFirst("^BIT_TO_NUM", "BIN_TO_NUM");
        case NULLIFZERO:
            return getNullZero(function, "NULLIF(");
        case ZEROIFNULL:
            return getNullZero(function, "IFNULL(");
        case DIV:
            return getDiv(function);
        default:
            return super.visit(function);
        }
    }

    private String getDiv(final SqlFunctionScalar function) throws AdapterException {
        final List<SqlNode> arguments = function.getArguments();
        final List<String> argumentsSql = new ArrayList<>(arguments.size());
        for (final SqlNode node : arguments) {
            argumentsSql.add(node.accept(this));
        }
        final StringBuilder builder = new StringBuilder();
        builder.append("CAST(FLOOR(");
        builder.append(argumentsSql.get(0));
        builder.append(" / FLOOR(");
        builder.append(argumentsSql.get(1));
        builder.append(")) AS DECIMAL(36, 0))");
        return builder.toString();
    }

    private String getNullZero(final SqlFunctionScalar function, final String expression) throws AdapterException {
        final List<String> argumentsSql = new ArrayList<>();
        for (final SqlNode node : function.getArguments()) {
            argumentsSql.add(node.accept(this));
        }
        final StringBuilder builder = new StringBuilder();
        builder.append(expression);
        builder.append(argumentsSql.get(0));
        builder.append(", 0)");
        return builder.toString();
    }

    private String getAddTimeOrDate(final SqlFunctionScalar function) throws AdapterException {
        final List<String> argumentsSql = new ArrayList<>();
        for (final SqlNode node : function.getArguments()) {
            argumentsSql.add(node.accept(this));
        }
        final StringBuilder builder = new StringBuilder();
        final SqlColumn column = (SqlColumn) function.getArguments().get(0);
        final String typeName = getTypeNameFromColumn(column);
        boolean isTimestamp = false; // special cast required
        if (typeName.contains("TIMESTAMP")) {
            isTimestamp = true;
            builder.append("VARCHAR(");
        }
        builder.append(argumentsSql.get(0));
        builder.append(" + ");
        if (function.getFunction() == ScalarFunction.ADD_WEEKS) {
            builder.append(7 * Integer.parseInt(argumentsSql.get(1)));
        } else {
            builder.append(argumentsSql.get(1));
        }
        builder.append(" ");
        switch (function.getFunction()) {
        case ADD_DAYS:
        case ADD_WEEKS:
            builder.append("DAYS");
            break;
        case ADD_HOURS:
            builder.append("HOURS");
            break;
        case ADD_MINUTES:
            builder.append("MINUTES");
            break;
        case ADD_SECONDS:
            builder.append("SECONDS");
            break;
        case ADD_YEARS:
            builder.append("YEARS");
            break;
        default:
            break;
        }
        if (isTimestamp) {
            builder.append(")");
        }
        return builder.toString();
    }

    private String getTrim(final SqlFunctionScalar function) throws AdapterException {
        final List<String> argumentsSql = new ArrayList<>();
        for (final SqlNode node : function.getArguments()) {
            argumentsSql.add(node.accept(this));
        }
        final StringBuilder builder = new StringBuilder();
        builder.append("TRIM(");
        if (argumentsSql.size() > 1) {
            builder.append(argumentsSql.get(1));
            builder.append(" FROM ");
            builder.append(argumentsSql.get(0));
        } else {
            builder.append(argumentsSql.get(0));
        }
        builder.append(")");
        return builder.toString();
    }

    @Override
    public String visit(final SqlFunctionAggregate function) throws AdapterException {
        final String sql = super.visit(function);
        if (function.getFunction() == AggregateFunction.VAR_SAMP) {
            return sql.replaceFirst("^VAR_SAMP", "VARIANCE_SAMP");
        } else {
            return sql;
        }
    }

    @Override
    public String visit(final SqlFunctionAggregateGroupConcat function) throws AdapterException {
        final StringBuilder builder = new StringBuilder();
        builder.append("LISTAGG");
        builder.append("(");
        if (function.getArgument() != null) {
            return getGroupConcat(function, builder);
        } else {
            throw new SqlGenerationVisitorException(
                    "Arguments of SqlFunctionAggregateGroupConcat shouldn't be null or empty.");
        }
    }

    private String getGroupConcat(final SqlFunctionAggregateGroupConcat function, final StringBuilder builder)
            throws AdapterException {
        final String expression = function.getArgument().accept(this);
        builder.append(expression);
        builder.append(", ");
        final String separator = function.getSeparator() == null ? "','" : function.getSeparator().accept(this);
        builder.append(separator);
        builder.append(") ");
        builder.append("WITHIN GROUP(ORDER BY ");
        if (function.hasOrderBy()) {
            getOrderBy(function, builder);
        } else {
            builder.append(expression);
        }
        builder.append(")");
        return builder.toString();
    }

    private void getOrderBy(final SqlFunctionAggregateGroupConcat function, final StringBuilder builder)
            throws AdapterException {
        for (int i = 0; i < function.getOrderBy().getExpressions().size(); i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(function.getOrderBy().getExpressions().get(i).accept(this));
            if (!function.getOrderBy().isAscending().get(i)) {
                builder.append(" DESC");
            }
        }
    }
}
