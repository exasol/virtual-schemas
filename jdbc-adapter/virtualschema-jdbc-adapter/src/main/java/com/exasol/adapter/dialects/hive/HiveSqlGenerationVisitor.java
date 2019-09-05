package com.exasol.adapter.dialects.hive;

import java.util.*;
import java.util.function.Predicate;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.ColumnAdapterNotes;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.sql.*;

/**
 * This class generates SQL queries for the {@link HiveSqlDialect}.
 */
public class HiveSqlGenerationVisitor extends SqlGenerationVisitor {
    private static final String BINARY_TYPE_NAME = "BINARY";

    /**
     * Create a new instance of the {@link HiveSqlGenerationVisitor}.
     *
     * @param dialect {@link HiveSqlDialect} SQL dialect
     * @param context SQL generation context
     */
    public HiveSqlGenerationVisitor(final SqlDialect dialect, final SqlGenerationContext context) {
        super(dialect, context);
    }

    @Override
    public String visit(final SqlSelectList selectList) throws AdapterException {
        final List<String> selectListElements = new ArrayList<>();
        if (selectList.isSelectStar()) {
            selectListElements.addAll(getSelectStarList(selectList));
        } else {
            if (selectList.isRequestAnyColumn()) {
                return SqlConstants.ONE;
            } else {
                selectListElements.addAll(getSelectList(selectList));
            }
        }
        return String.join(", ", selectListElements);
    }

    private List<String> getSelectStarList(final SqlSelectList selectList) throws AdapterException {
        if (SqlGenerationHelper.selectListRequiresCasts(selectList, nodeRequiresCast)) {
            final List<TableMetadata> tableMetadata = new ArrayList<>();
            final SqlStatementSelect select = (SqlStatementSelect) selectList.getParent();
            SqlGenerationHelper.addMetadata(select.getFromClause(), tableMetadata);
            return getSelectListWithColumns(tableMetadata);
        } else {
            return new ArrayList<>(Collections.singletonList("*"));
        }
    }

    private List<String> getSelectListWithColumns(final List<TableMetadata> tableMetadata) throws AdapterException {
        final List<String> selectListElements = new ArrayList<>(tableMetadata.size());
        int columnId = 0;
        for (final TableMetadata tableMeta : tableMetadata) {
            for (final ColumnMetadata columnMeta : tableMeta.getColumns()) {
                final SqlColumn sqlColumn = new SqlColumn(columnId, columnMeta);
                final String typeName = getTypeName(sqlColumn);
                if (typeName.equals(BINARY_TYPE_NAME)) {
                    selectListElements.add("base64(" + super.visit(sqlColumn) + ")");
                } else {
                    selectListElements.add(super.visit(sqlColumn));
                }
                ++columnId;
            }
        }
        return selectListElements;
    }

    private String getTypeName(final SqlColumn sqlColumn) throws AdapterException {
        return ColumnAdapterNotes
                .deserialize(sqlColumn.getMetadata().getAdapterNotes(), sqlColumn.getMetadata().getName())
                .getTypeName();
    }

    private List<String> getSelectList(final SqlSelectList selectList) throws AdapterException {
        final List<SqlNode> expressions = selectList.getExpressions();
        final List<String> selectListElements = new ArrayList<>(expressions.size());
        for (final SqlNode node : expressions) {
            if (node.getType().equals(SqlNodeType.COLUMN)) {
                final SqlColumn sqlColumn = (SqlColumn) node;
                final String typeName = getTypeName(sqlColumn);
                if (typeName.equals(BINARY_TYPE_NAME)) {
                    selectListElements.add("base64(" + node.accept(this) + ")");
                } else {
                    selectListElements.add(node.accept(this));
                }
            } else {
                selectListElements.add(node.accept(this));
            }
        }
        return selectListElements;
    }

    @Override
    public String visit(final SqlPredicateEqual function) throws AdapterException {
        if (function.getLeft().accept(this).equalsIgnoreCase("NULL")) {
            return getPredicateEqualityProjection(function.getRight(), " IS NULL");
        } else if (function.getRight().accept(this).equalsIgnoreCase("NULL")) {
            return getPredicateEqualityProjection(function.getLeft(), " IS NULL");
        } else {
            return super.visit(function);
        }
    }

    @Override
    public String visit(final SqlPredicateNotEqual function) throws AdapterException {
        if (function.getLeft().accept(this).equalsIgnoreCase("NULL")) {
            return getPredicateEqualityProjection(function.getRight(), " IS NOT NULL");
        } else if (function.getRight().accept(this).equalsIgnoreCase("NULL")) {
            return getPredicateEqualityProjection(function.getLeft(), " IS NOT NULL");
        } else {
            return super.visit(function);
        }
    }

    private String getPredicateEqualityProjection(final SqlNode sqlNode, final String string) throws AdapterException {
        return sqlNode.accept(this) + string;
    }

    @Override
    public String visit(final SqlPredicateLikeRegexp function) throws AdapterException {
        return function.getLeft().accept(this) + "REGEXP" + function.getPattern().accept(this);
    }

    @Override
    public String visit(final SqlFunctionScalar function) throws AdapterException {
        switch (function.getFunction()) {
        case CONCAT:
            return getCastedFunction("CONCAT", function);
        case REPEAT:
            return getCastedFunction("REPEAT", function);
        case UPPER:
            return getCastedFunction("UPPER", function);
        case LOWER:
            return getCastedFunction("LOWER", function);
        case DIV:
            return getChangedFunction(function, "DIV");
        case MOD:
            return getChangedFunction(function, "%");
        case SUBSTR:
            return getChangedSubstringFunction(function);
        case CURRENT_DATE:
            return "CURRENT_DATE";
        case DATE_TRUNC:
            return changeDateTrunc(function);
        case BIT_AND:
            return getChangedFunction(function, "&");
        case BIT_OR:
            return getChangedFunction(function, "|");
        case BIT_XOR:
            return getChangedFunction(function, "^");
        default:
            return super.visit(function);
        }
    }

    private String getCastedFunction(final String functionName, final SqlFunctionScalar function)
            throws AdapterException {
        final List<SqlNode> arguments = function.getArguments();
        final List<String> argumentsSql = new ArrayList<>(arguments.size());
        for (final SqlNode node : arguments) {
            argumentsSql.add(node.accept(this));
        }
        final StringBuilder builder = new StringBuilder();
        builder.append("CAST(").append(functionName).append("(");
        int i = 1;
        for (final String argument : argumentsSql) {
            builder.append(argument);
            if (argumentsSql.size() > i) {
                builder.append(",");
                i++;
            }
        }
        builder.append(") as string)");
        return builder.toString();
    }

    private String getChangedFunction(final SqlFunctionScalar function, final String replacement)
            throws AdapterException {
        final List<SqlNode> arguments = function.getArguments();
        final List<String> argumentsSql = new ArrayList<>(arguments.size());
        for (final SqlNode node : arguments) {
            argumentsSql.add(node.accept(this));
        }
        final StringBuilder builder = new StringBuilder();
        builder.append(argumentsSql.get(0));
        builder.append(" ");
        builder.append(replacement);
        builder.append(" ");
        builder.append(argumentsSql.get(1));
        return builder.toString();
    }

    private String getChangedSubstringFunction(final SqlFunctionScalar function) throws AdapterException {
        final List<SqlNode> arguments = function.getArguments();
        final List<String> argumentsSql = new ArrayList<>(arguments.size());
        for (final SqlNode node : arguments) {
            argumentsSql.add(node.accept(this));
        }
        if (function.toSimpleSql().toUpperCase().contains("FROM")) {
            final StringBuilder builder = new StringBuilder();
            builder.append("SUBSTRING(");
            builder.append(argumentsSql.get(0));
            builder.append(",");
            builder.append(argumentsSql.get(1));
            builder.append(")");
            return builder.toString();
        } else {
            return super.visit(function);
        }
    }

    private String changeDateTrunc(final SqlFunctionScalar function) throws AdapterException {
        final List<SqlNode> arguments = function.getArguments();
        final List<String> argumentsSql = new ArrayList<>(arguments.size());
        for (final SqlNode node : arguments) {
            argumentsSql.add(node.accept(this));
        }
        final StringBuilder builder = new StringBuilder();
        builder.append("TRUNC(");
        builder.append(argumentsSql.get(1));
        builder.append(",");
        builder.append(argumentsSql.get(0));
        builder.append(")");
        return builder.toString();
    }

    private final Predicate<SqlNode> nodeRequiresCast = node -> {
        try {
            if (node.getType() == SqlNodeType.COLUMN) {
                final SqlColumn column = (SqlColumn) node;
                final String typeName = getTypeName(column);
                return typeName.equals(BINARY_TYPE_NAME);
            }
            return false;
        } catch (Exception exception) {
            throw new SqlGenerationVisitorException("Exception during deserialization of ColumnAdapterNotes. ",
                    exception);
        }
    };
}
