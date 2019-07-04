package com.exasol.adapter.dialects.hive;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.ColumnAdapterNotes;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.sql.*;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

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
                return "1";
            } else {
                selectListElements.addAll(getSelectList(selectList));
            }
        }
        return Joiner.on(", ").join(selectListElements);
    }

    private List<String> getSelectStarList(final SqlSelectList selectList) throws AdapterException {
        final List<String> selectListElements = new ArrayList<>();
        if (SqlGenerationHelper.selectListRequiresCasts(selectList, nodeRequiresCast)) {
            final List<TableMetadata> tableMetadata = new ArrayList<>();
            final SqlStatementSelect select = (SqlStatementSelect) selectList.getParent();
            SqlGenerationHelper.addMetadata(select.getFromClause(), tableMetadata);
            addColumns(selectListElements, tableMetadata);
        } else {
            selectListElements.add("*");
        }
        return selectListElements;
    }

    private void addColumns(final List<String> selectListElements, final List<TableMetadata> tableMetadata)
            throws AdapterException {
        int columnId = 0;
        for (final TableMetadata tableMeta : tableMetadata) {
            for (final ColumnMetadata columnMeta : tableMeta.getColumns()) {
                final SqlColumn sqlColumn = new SqlColumn(columnId, columnMeta);
                final String typeName = ColumnAdapterNotes
                        .deserialize(sqlColumn.getMetadata().getAdapterNotes(), sqlColumn.getMetadata().getName())
                        .getTypeName();
                if (typeName.equals(BINARY_TYPE_NAME)) {
                    selectListElements.add("base64(" + super.visit(sqlColumn) + ")");
                } else {
                    selectListElements.add(super.visit(sqlColumn));
                }
                ++columnId;
            }
        }
    }

    private List<String> getSelectList(final SqlSelectList selectList) throws AdapterException {
        final List<String> selectListElements = new ArrayList<>();
        for (final SqlNode node : selectList.getExpressions()) {
            if (node.getType().equals(SqlNodeType.COLUMN)) {
                final SqlColumn sqlColumn = (SqlColumn) node;
                final String typeName = ColumnAdapterNotes
                        .deserialize(sqlColumn.getMetadata().getAdapterNotes(), sqlColumn.getMetadata().getName())
                        .getTypeName();
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
        String sql = super.visit(function);
        switch (function.getFunction()) {
        case CONCAT:
            sql = getCastedFunction("CONCAT", function);
            break;
        case REPEAT:
            sql = getCastedFunction("REPEAT", function);
            break;
        case UPPER:
            sql = getCastedFunction("UPPER", function);
            break;
        case LOWER:
            sql = getCastedFunction("LOWER", function);
            break;
        case DIV:
            sql = getChangedFunction(function, "DIV");
            break;
        case MOD:
            sql = getChangedFunction(function, "%");
            break;
        case SUBSTR:
            sql = getChangedSubstringFunction(function);
            break;
        case CURRENT_DATE:
            sql = "CURRENT_DATE";
            break;
        case DATE_TRUNC:
            sql = changeDateTrunc(function);
            break;
        case BIT_AND:
            sql = getChangedFunction(function, "&");
            break;
        case BIT_OR:
            sql = getChangedFunction(function, "|");
            break;
        case BIT_XOR:
            sql = getChangedFunction(function, "^");
            break;
        default:
            break;
        }
        return sql;
    }

    private String getCastedFunction(final String functionName, final SqlFunctionScalar function)
            throws AdapterException {
        final List<String> argumentsSql = new ArrayList<>();
        for (final SqlNode node : function.getArguments()) {
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
        final List<String> argumentsSql = new ArrayList<>();
        for (final SqlNode node : function.getArguments()) {
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
        final List<String> argumentsSql = new ArrayList<>();
        for (final SqlNode node : function.getArguments()) {
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
        final List<String> argumentsSql = new ArrayList<>();
        for (final SqlNode node : function.getArguments()) {
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
                SqlColumn column = (SqlColumn) node;
                String typeName = ColumnAdapterNotes
                        .deserialize(column.getMetadata().getAdapterNotes(), column.getMetadata().getName())
                        .getTypeName();
                return typeName.equals(BINARY_TYPE_NAME);
            }
            return false;
        } catch (Exception exception) {
            throw new SqlGenerationVisitorException("Exception during deserialization of ColumnAdapterNotes. ",
                    exception);
        }
    };
}
