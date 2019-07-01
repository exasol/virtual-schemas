package com.exasol.adapter.dialects.hive;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlGenerationContext;
import com.exasol.adapter.dialects.SqlGenerationHelper;
import com.exasol.adapter.dialects.SqlGenerationVisitor;
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
        final SqlStatementSelect select = (SqlStatementSelect) selectList.getParent();
        if (selectList.isSelectStar()) {
            if (SqlGenerationHelper.selectListRequiresCasts(selectList, nodeRequiresCast)) {
                // Do as if the user has all columns in select list
                int columnId = 0;
                final List<TableMetadata> tableMetadata = new ArrayList<>();
                SqlGenerationHelper.addMetadata(select.getFromClause(), tableMetadata );
                for (final TableMetadata tableMeta : tableMetadata) {
                    for (final ColumnMetadata columnMeta : tableMeta.getColumns()) {
                        final SqlColumn sqlColumn = new SqlColumn(columnId, columnMeta);
                        final String typeName = ColumnAdapterNotes.deserialize(sqlColumn.getMetadata().getAdapterNotes(), sqlColumn.getMetadata().getName()).getTypeName();
                        if (typeName.equals("BINARY")) {
                            selectListElements.add("base64(" + super.visit(sqlColumn) + ")");
                        } else {
                            selectListElements.add(super.visit(sqlColumn));
                        }
                        ++columnId;
                    }
                }
            }
            else {
                selectListElements.add("*");
            }
        } else {
            if(selectList.isRequestAnyColumn()){
                return "1";
            }
            for (final SqlNode node : selectList.getExpressions()) {
                if(node.getType().equals(SqlNodeType.COLUMN)) {
                    final SqlColumn sqlColumn = (SqlColumn) node;
                    final String typeName = ColumnAdapterNotes.deserialize(sqlColumn.getMetadata().getAdapterNotes(), sqlColumn.getMetadata().getName()).getTypeName();
                    if (typeName.equals("BINARY")) {
                        selectListElements.add("base64(" + node.accept(this) + ")");
                    } else {
                        selectListElements.add(node.accept(this));
                    }
                }
                else{
                    selectListElements.add(node.accept(this));
                }
            }
        }

        return Joiner.on(", ").join(selectListElements);
    }

    @Override
    public String visit(final SqlPredicateEqual function) throws AdapterException {
        String sql = super.visit(function);
        if(function.getLeft().accept(this).toUpperCase().equals("NULL")){
            final StringBuilder builder = new StringBuilder();
            builder.append(function.getRight().accept(this));
            builder.append(" IS NULL");
            sql = builder.toString();
        }
        else if(function.getRight().accept(this).toUpperCase().equals("NULL")){
            final StringBuilder builder = new StringBuilder();
            builder.append(function.getLeft().accept(this));
            builder.append(" IS NULL");
            sql = builder.toString();
        }
        return sql;
    }

    @Override
    public String visit(final SqlPredicateNotEqual function) throws AdapterException {
        String sql = super.visit(function);
        if(function.getLeft().accept(this).toUpperCase().equals("NULL")){
            final StringBuilder builder = new StringBuilder();
            builder.append(function.getRight().accept(this));
            builder.append(" IS NOT NULL");
            sql = builder.toString();
        }
        else if(function.getRight().accept(this).toUpperCase().equals("NULL")){
            final StringBuilder builder = new StringBuilder();
            builder.append(function.getLeft().accept(this));
            builder.append(" IS NOT NULL");
            sql = builder.toString();
        }
        return sql;
    }

    @Override
    public String visit(final SqlPredicateLikeRegexp function) throws AdapterException {
        return function.getLeft().accept(this) + "REGEXP" + function.getPattern().accept(this);
    }

    @Override
    public String visit(final SqlFunctionScalar function) throws AdapterException {
        String sql = super.visit(function);
        switch (function.getFunction()) {
            case CONCAT: {
                sql = getCastedFunction("CONCAT",function);
                break;
            }
            case REPEAT: {
                sql = getCastedFunction("REPEAT",function);
                break;
            }
            case UPPER: {
                sql = getCastedFunction("UPPER",function);
                break;
            }
            case LOWER: {
                sql = getCastedFunction("LOWER",function);
                break;
            }
            case DIV: {
                sql = getChangedFunction(function,"DIV");
                break;
            }
            case MOD: {
                sql = getChangedFunction(function,"%");
                break;
            }
            case SUBSTR:{
                sql = getChangedSubstringFunction(function);
                break;
            }
            case CURRENT_DATE:{
                sql = "CURRENT_DATE";
                break;
            }
            case DATE_TRUNC:{
                sql = changeDateTrunc(function);
                break;
            }
            case BIT_AND: {
                sql = getChangedFunction(function,"&");
                break;
            }
            case BIT_OR: {
                sql = getChangedFunction(function,"|");
                break;
            }
            case BIT_XOR: {
                sql = getChangedFunction(function,"^");
                break;
            }
            default:
                break;
        }

        return sql;
    }

    private String getCastedFunction(final String functionName, final SqlFunctionScalar function) throws AdapterException {
        final List<String> argumentsSql = new ArrayList<>();
        for (final SqlNode node : function.getArguments()) {
            argumentsSql.add(node.accept(this));
        }
        final StringBuilder builder = new StringBuilder();
        builder.append("CAST("+functionName+"(");
        Integer i=1;
        for(final String argument : argumentsSql){
            builder.append(argument);
            if(argumentsSql.size()>i){
                builder.append(",");
                i++;
            }
        }
        builder.append(") as string)");
        return builder.toString();
    }

    private String getChangedFunction(final SqlFunctionScalar function, final String replacement) throws AdapterException {
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
        if(function.toSimpleSql().toUpperCase().contains("FROM")){
            final StringBuilder builder = new StringBuilder();
            builder.append("SUBSTRING(");
            builder.append(argumentsSql.get(0));
            builder.append(",");
            builder.append(argumentsSql.get(1));
            builder.append(")");
            return builder.toString();
        }
        else{
            return super.visit(function);
        }
    }

    //change name to "TRUNC" and change the place of the arguments
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
                SqlColumn column = (SqlColumn)node;
                String typeName = ColumnAdapterNotes.deserialize(column.getMetadata().getAdapterNotes(),
                                                                 column.getMetadata().getName()).getTypeName();
                return typeName.equals("BINARY");
            }
            return false;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    };
}
