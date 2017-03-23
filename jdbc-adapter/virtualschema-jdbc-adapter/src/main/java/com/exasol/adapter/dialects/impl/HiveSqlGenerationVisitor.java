package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlGenerationContext;
import com.exasol.adapter.dialects.SqlGenerationVisitor;
import com.exasol.adapter.jdbc.ColumnAdapterNotes;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.sql.*;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.List;

public class HiveSqlGenerationVisitor extends SqlGenerationVisitor {


    public HiveSqlGenerationVisitor(SqlDialect dialect, SqlGenerationContext context) {
        super(dialect, context);
    }


    @Override
    public String visit(SqlSelectList selectList) throws AdapterException {
        List<String> selectListElements = new ArrayList<>();
        SqlStatementSelect select = (SqlStatementSelect) selectList.getParent();
        if (selectList.isSelectStar()) {
            if (selectListRequiresCasts(selectList)) {
                // Do as if the user has all columns in select list
                int columnId = 0;
                for (ColumnMetadata columnMeta : select.getFromClause().getMetadata().getColumns()) {
                    SqlColumn sqlColumn = new SqlColumn(columnId, columnMeta);
                    String typeName = ColumnAdapterNotes.deserialize(sqlColumn.getMetadata().getAdapterNotes(), sqlColumn.getMetadata().getName()).getTypeName();
                    if (typeName.equals("BINARY")) {
                        selectListElements.add("base64(" + super.visit(sqlColumn) + ")");
                    } else {
                        selectListElements.add(super.visit(sqlColumn));
                    }
                    ++columnId;
                }
            }
            else {
                selectListElements.add("*");
            }
        } else {
            if(selectList.isRequestAnyColumn()){
                return "1";
            }
            for (SqlNode node : selectList.getExpressions()) {
                if(node.getType().equals(SqlNodeType.COLUMN)) {
                    SqlColumn sqlColumn = (SqlColumn) node;
                    String typeName = ColumnAdapterNotes.deserialize(sqlColumn.getMetadata().getAdapterNotes(), sqlColumn.getMetadata().getName()).getTypeName();
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
    public String visit(SqlPredicateEqual function) throws AdapterException {
        String sql = super.visit(function);
        if(function.getLeft().accept(this).toUpperCase().equals("NULL")){
            StringBuilder builder = new StringBuilder();
            builder.append(function.getRight().accept(this));
            builder.append(" IS NULL");
            sql = builder.toString();
        }
        else if(function.getRight().accept(this).toUpperCase().equals("NULL")){
            StringBuilder builder = new StringBuilder();
            builder.append(function.getLeft().accept(this));
            builder.append(" IS NULL");
            sql = builder.toString();
        }
        return sql;
    }

    @Override
    public String visit(SqlPredicateNotEqual function) throws AdapterException {
        String sql = super.visit(function);
        if(function.getLeft().accept(this).toUpperCase().equals("NULL")){
            StringBuilder builder = new StringBuilder();
            builder.append(function.getRight().accept(this));
            builder.append(" IS NOT NULL");
            sql = builder.toString();
        }
        else if(function.getRight().accept(this).toUpperCase().equals("NULL")){
            StringBuilder builder = new StringBuilder();
            builder.append(function.getLeft().accept(this));
            builder.append(" IS NOT NULL");
            sql = builder.toString();
        }
        return sql;
    }

    @Override
    public String visit(SqlPredicateLikeRegexp function) throws AdapterException {
        return function.getLeft().accept(this) + "REGEXP" + function.getPattern().accept(this);
    }

    @Override
    public String visit(SqlFunctionScalar function) throws AdapterException {
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

    private String getCastedFunction(String functionName,SqlFunctionScalar function) throws AdapterException {
        List<String> argumentsSql = new ArrayList<>();
        for (SqlNode node : function.getArguments()) {
            argumentsSql.add(node.accept(this));
        }
        StringBuilder builder = new StringBuilder();
        builder.append("CAST("+functionName+"(");
        Integer i=1;
        for(String argument : argumentsSql){
            builder.append(argument);
            if(argumentsSql.size()>i){
                builder.append(",");
                i++;
            }
        }
        builder.append(") as string)");
        return builder.toString();
    }

    private String getChangedFunction(SqlFunctionScalar function,String replacement) throws AdapterException {
        List<String> argumentsSql = new ArrayList<>();
        for (SqlNode node : function.getArguments()) {
            argumentsSql.add(node.accept(this));
        }
        StringBuilder builder = new StringBuilder();
        builder.append(argumentsSql.get(0));
        builder.append(" ");
        builder.append(replacement);
        builder.append(" ");
        builder.append(argumentsSql.get(1));
        return builder.toString();
    }

    private String getChangedSubstringFunction(SqlFunctionScalar function) throws AdapterException {
        List<String> argumentsSql = new ArrayList<>();
        for (SqlNode node : function.getArguments()) {
            argumentsSql.add(node.accept(this));
        }
        if(function.toSimpleSql().toUpperCase().contains("FROM")){
            StringBuilder builder = new StringBuilder();
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
    private String changeDateTrunc(SqlFunctionScalar function) throws AdapterException {
        List<String> argumentsSql = new ArrayList<>();
        for (SqlNode node : function.getArguments()) {
            argumentsSql.add(node.accept(this));
        }
        StringBuilder builder = new StringBuilder();
        builder.append("TRUNC(");
        builder.append(argumentsSql.get(1));
        builder.append(",");
        builder.append(argumentsSql.get(0));
        builder.append(")");
        return builder.toString();
    }


    private boolean selectListRequiresCasts(SqlSelectList selectList) throws AdapterException {

        // Do as if the user has all columns in select list
        SqlStatementSelect select = (SqlStatementSelect) selectList.getParent();
        int columnId = 0;
        for (ColumnMetadata columnMeta : select.getFromClause().getMetadata().getColumns()) {
            SqlColumn sqlColumn = new SqlColumn(columnId, columnMeta);
            String typeName = ColumnAdapterNotes.deserialize(sqlColumn.getMetadata().getAdapterNotes(), sqlColumn.getMetadata().getName()).getTypeName();
            if(typeName.equals("BINARY")  ){
                return true;
            }
            ++columnId;
        }

        return false;
    }
}
