package com.exasol.adapter.dialects.db2;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.ColumnAdapterNotes;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.sql.*;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * This class generates SQL queries for the {@link DB2SqlDialect}.
 */
public class DB2SqlGenerationVisitor extends SqlGenerationVisitor {
    /**
     * Create a new instance of the {@link DB2SqlGenerationVisitor}.
     *
     * @param dialect {@link DB2SqlDialect} SQL dialect
     * @param context SQL generation context
     */
    public DB2SqlGenerationVisitor(final SqlDialect dialect, final SqlGenerationContext context) {
        super(dialect, context);
    }

    @Override
    public String visit(final SqlColumn column) throws AdapterException {
        final String projectionString = super.visit(column);
        return getColumnProjectionString(column, projectionString);
    }

    private String getColumnProjectionString(final SqlColumn column, final String projectionString)
            throws AdapterException {
        final boolean isDirectlyInSelectList = checkIfColumnIsDirectlyInSelectList(column);
        if (!isDirectlyInSelectList) {
            return projectionString;
        } else {
            final String typeName = ColumnAdapterNotes
                    .deserialize(column.getMetadata().getAdapterNotes(), column.getMetadata().getName()).getTypeName();
            return getColumnProjectionStringNoCheck(typeName, projectionString);
        }
    }

    private boolean checkIfColumnIsDirectlyInSelectList(final SqlColumn column) {
        return column.hasParent() && column.getParent().getType() == SqlNodeType.SELECT_LIST;
    }

    private String getColumnProjectionStringNoCheck(final String typeName, String projectionString) {
        if (TYPE_NAME_NOT_SUPPORTED.contains(typeName)) {
            projectionString = "'" + typeName + " NOT SUPPORTED'";
        } else {
            projectionString = getProjectionStringWithSupportedTypes(typeName, projectionString);
        }
        return projectionString;
    }

    private String getProjectionStringWithSupportedTypes(final String typeName, String projectionString) {
        switch (typeName) {
        case "XML":
            projectionString = "XMLSERIALIZE(" + projectionString + " as VARCHAR(32000) INCLUDING XMLDECLARATION)";
            break;
        // db2 does not support cast of clobs to varchar in full length -> max 32672
        case "CLOB":
            projectionString = "CAST(SUBSTRING(" + projectionString + ",32672) AS VARCHAR(32672))";
            break;
        case "CHAR () FOR BIT DATA":
        case "VARCHAR () FOR BIT DATA":
            projectionString = "HEX(" + projectionString + ")";
            break;
        case "TIME":
            // cast timestamp to not lose precision
        case "TIMESTAMP":
            projectionString = "VARCHAR(" + projectionString + ")";
            break;
        default:
            break;
        }
        return projectionString;
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
    public String visit(final SqlSelectList selectList) throws AdapterException {
        if (selectList.isRequestAnyColumn()) {
            // The system requested any column
            return "1";
        } else {
            return getSelectList(selectList);
        }
    }

    private String getSelectList(final SqlSelectList selectList) throws AdapterException {
        final List<String> selectListElements = new ArrayList<>();
        if (selectList.isSelectStar()) {
            buildSelectStar(selectList, selectListElements);
        } else {
            for (final SqlNode node : selectList.getExpressions()) {
                selectListElements.add(node.accept(this));
            }
        }
        return Joiner.on(", ").join(selectListElements);
    }

    private void buildSelectStar(final SqlSelectList selectList, final List<String> selectListElements)
            throws AdapterException {
        if (SqlGenerationHelper.selectListRequiresCasts(selectList, nodeRequiresCast)) {
            buildSelectStarWithNodeCast(selectList, selectListElements);
        } else {
            selectListElements.add("*");
        }
    }

    private void buildSelectStarWithNodeCast(final SqlSelectList selectList, final List<String> selectListElements)
            throws AdapterException {
        final SqlStatementSelect select = (SqlStatementSelect) selectList.getParent();
        int columnId = 0;
        final List<TableMetadata> tableMetadata = new ArrayList<>();
        SqlGenerationHelper.addMetadata(select.getFromClause(), tableMetadata);
        for (final TableMetadata tableMeta : tableMetadata) {
            for (final ColumnMetadata columnMeta : tableMeta.getColumns()) {
                final SqlColumn sqlColumn = new SqlColumn(columnId, columnMeta);
                selectListElements.add(getColumnProjectionStringNoCheck(sqlColumn, super.visit(sqlColumn)));
                ++columnId;
            }
        }
    }

    private String getColumnProjectionStringNoCheck(final SqlColumn column, final String projectionString)
            throws AdapterException {
        final String typeName = ColumnAdapterNotes
                .deserialize(column.getMetadata().getAdapterNotes(), column.getMetadata().getName()).getTypeName();
        return getColumnProjectionStringNoCheck(typeName, projectionString);
    }

    @Override
    public String visit(final SqlFunctionScalar function) throws AdapterException {
        String sql = super.visit(function);
        switch (function.getFunction()) {
        case TRIM:
            sql = getTrim(function);
            break;
        case ADD_DAYS:
        case ADD_HOURS:
        case ADD_MINUTES:
        case ADD_SECONDS:
        case ADD_WEEKS:
        case ADD_YEARS:
            sql = getAddTimeOrDate(function);
            break;
        case CURRENT_DATE:
        case SYSDATE:
            sql = "CURRENT DATE";
            break;
        case CURRENT_TIMESTAMP:
        case SYSTIMESTAMP:
            sql = "VARCHAR(CURRENT TIMESTAMP)";
            break;
        case DBTIMEZONE:
            sql = "DBTIMEZONE";
            break;
        case LOCALTIMESTAMP:
            sql = "LOCALTIMESTAMP";
            break;
        case SESSIONTIMEZONE:
            sql = "SESSIONTIMEZONE";
            break;
        case BIT_AND:
            sql = sql.replaceFirst("^BIT_AND", "BITAND");
            break;
        case BIT_TO_NUM:
            sql = sql.replaceFirst("^BIT_TO_NUM", "BIN_TO_NUM");
            break;
        case NULLIFZERO:
            sql = getNullZero(function, "NULLIF(");
            break;
        case ZEROIFNULL:
            sql = getNullZero(function, "IFNULL(");
            break;
        case DIV:
            sql = getDiv(function);
            break;
        default:
            break;
        }
        return sql;
    }

    private String getDiv(final SqlFunctionScalar function) throws AdapterException {
        final List<String> argumentsSql = new ArrayList<>();
        for (final SqlNode node : function.getArguments()) {
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
        final String typeName = ColumnAdapterNotes
                .deserialize(column.getMetadata().getAdapterNotes(), column.getMetadata().getName()).getTypeName();
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
        if (function.getArguments() != null && function.getArguments().size() == 1
                && function.getArguments().get(0) != null) {
            return getGroupConcat(function, builder);
        } else {
            throw new SqlGenerationVisitorException(
                    "Arguments of SqlFunctionAggregateGroupConcat shouldn't be null or empty.");
        }
    }

    private String getGroupConcat(final SqlFunctionAggregateGroupConcat function, final StringBuilder builder)
            throws AdapterException {
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

    private static final List<String> TYPE_NAMES_REQUIRING_CAST = ImmutableList.of("TIMESTAMP", "DECFLOAT", "CLOB",
            "XML", "TIME");
    private static final List<String> TYPE_NAME_NOT_SUPPORTED = ImmutableList.of("BLOB");

    private final java.util.function.Predicate<SqlNode> nodeRequiresCast = node -> {
        try {
            if (node.getType() == SqlNodeType.COLUMN) {
                SqlColumn column = (SqlColumn) node;
                String typeName = ColumnAdapterNotes
                        .deserialize(column.getMetadata().getAdapterNotes(), column.getMetadata().getName())
                        .getTypeName();
                return TYPE_NAMES_REQUIRING_CAST.contains(typeName);
            }
            return false;
        } catch (AdapterException exception) {
            throw new SqlGenerationVisitorException("Exception during deserialization of ColumnAdapterNotes. ",
                    exception);
        }
    };
}
