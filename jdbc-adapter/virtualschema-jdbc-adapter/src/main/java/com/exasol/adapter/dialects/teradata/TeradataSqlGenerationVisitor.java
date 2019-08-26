package com.exasol.adapter.dialects.teradata;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.ColumnAdapterNotes;
import com.exasol.adapter.metadata.*;
import com.exasol.adapter.sql.*;

/**
 * This class generates SQL queries for the {@link TeradataSqlDialect}.
 */
public class TeradataSqlGenerationVisitor extends SqlGenerationVisitor {
    private static final List<String> TYPE_NAMES_REQUIRING_CAST = List.of("SYSUDTLIB.ST_GEOMETRY", "XML", "JSON",
            "TIME", "TIME WITH TIME ZONE", "CLOB", "PERIOD", "INTERVAL");
    private static final List<String> TYPE_NAME_NOT_SUPPORTED = List.of("BYTE", "VARBYTE", "BLOB", "SYSUDTLIB");
    private final Predicate<SqlNode> nodeRequiresCast = node -> {
        try {
            if (node.getType() == SqlNodeType.COLUMN) {
                final SqlColumn column = (SqlColumn) node;
                final String typeName = ColumnAdapterNotes
                        .deserialize(column.getMetadata().getAdapterNotes(), column.getMetadata().getName())
                        .getTypeName();
                return TYPE_NAMES_REQUIRING_CAST.contains(typeName) || TYPE_NAME_NOT_SUPPORTED.contains(typeName)
                        || (typeName.startsWith("NUMBER")
                                && (column.getMetadata().getType().getExaDataType() == DataType.ExaDataType.DOUBLE));
            }
            return false;
        } catch (final AdapterException adapterException) {
            throw new SqlGenerationVisitorException("Deserialization failed.", adapterException);
        }
    };

    /**
     * Create a new instance of the {@link TeradataSqlGenerationVisitor}.
     *
     * @param dialect instance of {@link TeradataSqlDialect}
     * @param context SQL generation context
     */
    public TeradataSqlGenerationVisitor(final SqlDialect dialect, final SqlGenerationContext context) {
        super(dialect, context);
    }

    @Override
    public String visit(final SqlSelectList selectList) throws AdapterException {
        if (selectList.isRequestAnyColumn()) {
            return "1";
        }
        final List<String> selectListElements = new ArrayList<>();
        if (selectList.isSelectStar()) {
            if (SqlGenerationHelper.selectListRequiresCasts(selectList, this.nodeRequiresCast)) {
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
            } else {
                selectListElements.add("*");
            }
        } else {
            for (final SqlNode node : selectList.getExpressions()) {
                selectListElements.add(node.accept(this));
            }
        }
        return String.join(", ", selectListElements);
    }

    @Override
    public String visit(final SqlStatementSelect select) throws AdapterException {
        if (!select.hasLimit()) {
            return super.visit(select);
        } else {
            final SqlLimit limit = select.getLimit();
            final StringBuilder sql = new StringBuilder();
            sql.append("SELECT TOP ").append(limit.getLimit()).append(" ");
            sql.append(select.getSelectList().accept(this));
            sql.append(" FROM ");
            sql.append(select.getFromClause().accept(this));
            if (select.hasFilter()) {
                sql.append(" WHERE ");
                sql.append(select.getWhereClause().accept(this));
            }
            if (select.hasGroupBy()) {
                sql.append(" GROUP BY ");
                sql.append(select.getGroupBy().accept(this));
            }
            if (select.hasHaving()) {
                sql.append(" HAVING ");
                sql.append(select.getHaving().accept(this));
            }
            if (select.hasOrderBy()) {
                sql.append(" ");
                sql.append(select.getOrderBy().accept(this));
            }
            return sql.toString();
        }
    }

    @Override
    public String visit(final SqlColumn column) throws AdapterException {
        return getColumnProjectionString(column, super.visit(column));
    }

    private String getColumnProjectionString(final SqlColumn column, final String projString) throws AdapterException {
        final boolean isDirectlyInSelectList = (column.hasParent()
                && (column.getParent().getType() == SqlNodeType.SELECT_LIST));
        if (!isDirectlyInSelectList) {
            return projString;
        }
        final String typeName = ColumnAdapterNotes
                .deserialize(column.getMetadata().getAdapterNotes(), column.getMetadata().getName()).getTypeName();
        return getColumnProjectionStringNoCheckImpl(typeName, column, projString);

    }

    private String getColumnProjectionStringNoCheck(final SqlColumn column, final String projString)
            throws AdapterException {
        final String typeName = ColumnAdapterNotes
                .deserialize(column.getMetadata().getAdapterNotes(), column.getMetadata().getName()).getTypeName();
        return getColumnProjectionStringNoCheckImpl(typeName, column, projString);

    }

    private String getColumnProjectionStringNoCheckImpl(final String typeName, final SqlColumn column,
            String projString) {
        if (typeName.startsWith("SYSUDTLIB.ST_GEOMETRY") || typeName.startsWith("JSON")) {
            projString = "CAST(" + projString + "  as VARCHAR(" + TeradataColumnMetadataReader.MAX_TERADATA_VARCHAR_SIZE
                    + ") )";
        } else if (typeName.startsWith("XML")) {
            projString = "XMLSERIALIZE(DOCUMENT " + projString + " as VARCHAR("
                    + TeradataColumnMetadataReader.MAX_TERADATA_VARCHAR_SIZE + ") INCLUDING XMLDECLARATION) ";
        } else if (typeName.startsWith("NUMBER")
                && (column.getMetadata().getType().getExaDataType() == DataType.ExaDataType.DOUBLE)) {
            projString = "CAST(" + projString + "  as DOUBLE PRECISION)";
        } else if (typeName.equals("TIME") || typeName.equals("TIME WITH TIME ZONE")) {
            projString = "CAST(" + projString + "  as VARCHAR(21) )";
        } else if (typeName.startsWith("INTERVAL")) {
            projString = "CAST(" + projString + "  as VARCHAR(30) )";
        } else if (typeName.startsWith("PERIOD")) {
            projString = "CAST(" + projString + "  as VARCHAR(100) )";
        } else if (typeName.startsWith("CLOB")) {
            projString = "CAST(" + projString + "  as VARCHAR(" + TeradataColumnMetadataReader.MAX_TERADATA_VARCHAR_SIZE
                    + ") )";
        } else if (TYPE_NAME_NOT_SUPPORTED.contains(typeName)) {
            projString = "'" + typeName + " NOT SUPPORTED'";
        } else if (typeName.startsWith("SYSUDTLIB")) {
            projString = "'" + typeName + " NOT SUPPORTED'";
        }
        return projString;
    }
}
