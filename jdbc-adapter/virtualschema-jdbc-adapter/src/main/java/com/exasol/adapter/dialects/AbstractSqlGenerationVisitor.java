package com.exasol.adapter.dialects;

import java.util.*;

import com.exasol.adapter.*;
import com.exasol.adapter.jdbc.*;
import com.exasol.adapter.metadata.*;
import com.exasol.adapter.sql.*;
import com.google.common.base.*;

/**
 * This class contains common logic from the next dialects: DB2, PostgreSQL. It helps to avoid code duplication.
 */
public abstract class AbstractSqlGenerationVisitor extends SqlGenerationVisitor {
    /**
     * Creates a new instance of the {@link SqlGenerationVisitor}.
     *
     * @param dialect SQl dialect
     * @param context SQL generation context
     */
    public AbstractSqlGenerationVisitor(final SqlDialect dialect, final SqlGenerationContext context) {
        super(dialect, context);
    }

    protected abstract List<String> getListOfTypeNamesRequiringCast();

    protected abstract List<String> getListOfTypeNamesNotSupported();

    protected abstract String buildColumnProjectionString(final String typeName, String projectionString);

    @Override
    public String visit(final SqlSelectList selectList) throws AdapterException {
        if (selectList.isRequestAnyColumn()) {
            return "1";
        } else {
            return getSelectList(selectList);
        }
    }

    protected String getSelectList(final SqlSelectList selectList) throws AdapterException {
        final List<String> selectListElements = new ArrayList<>();
        if (selectList.isSelectStar()) {
            final List<String> selectStarList = buildSelectStar(selectList);
            selectListElements.addAll(selectStarList);
        } else {
            for (final SqlNode node : selectList.getExpressions()) {
                selectListElements.add(node.accept(this));
            }
        }
        return Joiner.on(", ").join(selectListElements);
    }

    protected List<String> buildSelectStar(final SqlSelectList selectList) throws AdapterException {
        final List<String> selectListElements = new ArrayList<>();
        if (SqlGenerationHelper.selectListRequiresCasts(selectList, this.nodeRequiresCast)) {
            buildSelectStarWithNodeCast(selectList, selectListElements);
        } else {
            selectListElements.add("*");
        }
        return selectListElements;
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
                selectListElements.add(buildColumnProjectionString(sqlColumn, super.visit(sqlColumn)));
                ++columnId;
            }
        }
    }

    private String buildColumnProjectionString(final SqlColumn column, final String projectionString)
            throws AdapterException {
        final String typeName = ColumnAdapterNotes
                .deserialize(column.getMetadata().getAdapterNotes(), column.getMetadata().getName()).getTypeName();
        return buildColumnProjectionString(typeName, projectionString);
    }

    protected final java.util.function.Predicate<SqlNode> nodeRequiresCast = node -> {
        try {
            if (node.getType() == SqlNodeType.COLUMN) {
                SqlColumn column = (SqlColumn) node;
                String typeName = ColumnAdapterNotes
                        .deserialize(column.getMetadata().getAdapterNotes(), column.getMetadata().getName())
                        .getTypeName();
                return getListOfTypeNamesRequiringCast().contains(typeName)
                        || getListOfTypeNamesNotSupported().contains(typeName);
            }
            return false;
        } catch (AdapterException exception) {
            throw new SqlGenerationVisitorException("Exception during deserialization of ColumnAdapterNotes. ",
                    exception);
        }
    };
}
