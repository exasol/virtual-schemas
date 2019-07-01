package com.exasol.adapter.dialects.db2;

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
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class generates SQL queries for the {@link DB2SqlDialect}.
 */
public class DB2SqlGenerationVisitor extends SqlGenerationVisitor {
    private final Set<ScalarFunction> scalarFunctionsCast = new HashSet<>();

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
        return getColumnProjectionString(column, super.visit(column));
    }

    private String getColumnProjectionString(final SqlColumn column, final String projString) throws AdapterException {
        final boolean isDirectlyInSelectList = (column.hasParent() && column.getParent().getType() == SqlNodeType.SELECT_LIST);
        if (!isDirectlyInSelectList) {
            return projString;
        }
        final String typeName = ColumnAdapterNotes.deserialize(column.getMetadata().getAdapterNotes(), column.getMetadata().getName()).getTypeName();
        return getColumnProjectionStringNoCheckImpl(typeName, column, projString);
    }


    private String getColumnProjectionStringNoCheck(final SqlColumn column, final String projString) throws AdapterException {

        final String typeName = ColumnAdapterNotes.deserialize(column.getMetadata().getAdapterNotes(), column.getMetadata().getName()).getTypeName();
        return getColumnProjectionStringNoCheckImpl(typeName, column, projString);
    }


    private String getColumnProjectionStringNoCheckImpl(final String typeName, final SqlColumn column, String projString) {

            switch (typeName) {
                case "XML":
            projString = "XMLSERIALIZE(" + projString + " as VARCHAR(32000) INCLUDING XMLDECLARATION)";
                        break;
                //db2 does not support cast of clobs to varchar in full length  -> max 32672
                case "CLOB":
                        projString = "CAST(SUBSTRING(" + projString + ",32672) AS VARCHAR(32672))";
                        break;
                case "CHAR () FOR BIT DATA":
                case "VARCHAR () FOR BIT DATA":
                        projString = "HEX(" + projString + ")";
                        break;
                case "TIME":
                // cast timestamp to not lose precision
                case "TIMESTAMP":
                        projString = "VARCHAR("+ projString + ")";
                        break;
                default:
                        break;
                }

        if (TYPE_NAME_NOT_SUPPORTED.contains(typeName)){

                projString = "'"+typeName+" NOT SUPPORTED'"; //returning a string constant for unsupported data types

        }

        return projString;
    }

    @Override
    public String visit(final SqlStatementSelect select) throws AdapterException {
        if (!select.hasLimit()) {
            return super.visit(select);
        } else {
                final SqlLimit limit = select.getLimit();

            final StringBuilder sql = new StringBuilder();
            sql.append("SELECT ");

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
            sql.append(" FETCH FIRST " + limit.getLimit() + " ROWS ONLY");
            return sql.toString();
        }
    }


    @Override
    public String visit(final SqlSelectList selectList) throws AdapterException {
        if (selectList.isRequestAnyColumn()) {
            // The system requested any column
            return "1";
        }
        final List<String> selectListElements = new ArrayList<>();
        if (selectList.isSelectStar()) {
            if (SqlGenerationHelper.selectListRequiresCasts(selectList, nodeRequiresCast)) {

                // Do as if the user has all columns in select list
                final SqlStatementSelect select = (SqlStatementSelect) selectList.getParent();

                int columnId = 0;
                final List<TableMetadata> tableMetadata = new ArrayList<TableMetadata>();
                SqlGenerationHelper.addMetadata(select.getFromClause(), tableMetadata );
                for (final TableMetadata tableMeta : tableMetadata) {
                    for (final ColumnMetadata columnMeta : tableMeta.getColumns()) {
                        final SqlColumn sqlColumn = new SqlColumn(columnId, columnMeta);
                        selectListElements.add( getColumnProjectionStringNoCheck(sqlColumn,  super.visit(sqlColumn)  )   );
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

        return Joiner.on(", ").join(selectListElements);
    }

        @Override
        public String visit(final SqlFunctionScalar function) throws AdapterException {
        String sql = super.visit(function);

                switch (function.getFunction()) {
        case TRIM: {
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
            sql = builder.toString();
            break;
                }
        case ADD_DAYS:
        case ADD_HOURS:
        case ADD_MINUTES:
        case ADD_SECONDS:
        case ADD_WEEKS:
        case ADD_YEARS: {
            final List<String> argumentsSql = new ArrayList<>();
            Boolean isTimestamp = false; //special cast required

            for (final SqlNode node : function.getArguments()) {
                argumentsSql.add(node.accept(this));
            }
            final StringBuilder builder = new StringBuilder();
            final SqlColumn column = (SqlColumn) function.getArguments().get(0);
            final String typeName = ColumnAdapterNotes.deserialize(column.getMetadata().getAdapterNotes(), column.getMetadata().getName()).getTypeName();
            System.out.println("!DB2 : " + typeName);
            if (typeName.contains("TIMESTAMP"))
                {
                    isTimestamp = true;
                    System.out.println("!DB2 : we got a timestamp");
                    builder.append("VARCHAR(");
                }

            builder.append(argumentsSql.get(0));
            builder.append(" + ");
            builder.append(argumentsSql.get(1));
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
            if (isTimestamp)
            {
                    builder.append(")");
            }
            sql = builder.toString();
            break;
                }
        case CURRENT_DATE:
            sql = "CURRENT DATE";
            break;
        case CURRENT_TIMESTAMP:
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
        case SYSDATE:
            sql = "CURRENT DATE";
            break;
        case SYSTIMESTAMP:
                    sql = "VARCHAR(CURRENT TIMESTAMP)";
                    break;
        case BIT_AND:
                    sql = sql.replaceFirst("^BIT_AND", "BITAND");
                    break;
        case BIT_TO_NUM:
                    sql = sql.replaceFirst("^BIT_TO_NUM", "BIN_TO_NUM");
                    break;
        case NULLIFZERO: {
                    final List<String> argumentsSql = new ArrayList<>();
                    for (final SqlNode node : function.getArguments()) {
                        argumentsSql.add(node.accept(this));
                    }
                    final StringBuilder builder = new StringBuilder();
                    builder.append("NULLIF(");
                    builder.append(argumentsSql.get(0));
                    builder.append(", 0)");
                    sql = builder.toString();
                    break;
                }
        case ZEROIFNULL: {
                    final List<String> argumentsSql = new ArrayList<>();
                    for (final SqlNode node : function.getArguments()) {
                        argumentsSql.add(node.accept(this));
                    }
                    final StringBuilder builder = new StringBuilder();
                    builder.append("IFNULL(");
                    builder.append(argumentsSql.get(0));
                    builder.append(", 0)");
                    sql = builder.toString();
                    break;
                }
        case DIV: {
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
                    sql = builder.toString();
                    break;
                }
        default:
            break;
        }

        final boolean isDirectlyInSelectList = (function.hasParent() && function.getParent().getType() == SqlNodeType.SELECT_LIST);
        if (isDirectlyInSelectList && scalarFunctionsCast.contains(function.getFunction())) {
            // Cast to FLOAT because result set metadata has precision = 0, scale = 0
            sql = "CAST("  + sql + " AS FLOAT)";
        }

        return sql;
    }


        @Override
        public String visit(final SqlFunctionAggregate function) throws AdapterException {
                String sql = super.visit(function);

                switch (function.getFunction()) {
                case VAR_SAMP:
                        sql = sql.replaceFirst("^VAR_SAMP", "VARIANCE_SAMP");
                        break;
                default:
                        break;
                }

                return sql;
        }


    @Override
    public String visit(final SqlFunctionAggregateGroupConcat function) throws AdapterException {
        final StringBuilder builder = new StringBuilder();
        builder.append("LISTAGG");
        builder.append("(");
        assert(function.getArguments() != null);
        assert(function.getArguments().size() == 1 && function.getArguments().get(0) != null);
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
            for (int i = 0; i < function.getOrderBy().getExpressions().size(); i++) {
                if (i > 0) {
                    builder.append(", ");
                }
                builder.append(function.getOrderBy().getExpressions().get(i).accept(this));
                if (!function.getOrderBy().isAscending().get(i)) {
                    builder.append(" DESC");
                }
            }
        } else {
            builder.append(expression);
        }
        builder.append(")");
        return builder.toString();
    }


    private static final List<String> TYPE_NAMES_REQUIRING_CAST = ImmutableList.of("TIMESTAMP","DECFLOAT","CLOB","XML","TIME");
    private static final List<String>  TYPE_NAME_NOT_SUPPORTED =  ImmutableList.of("BLOB");

    private final java.util.function.Predicate<SqlNode> nodeRequiresCast = node -> {
        try {
            if (node.getType() == SqlNodeType.COLUMN) {
                SqlColumn column = (SqlColumn)node;
                String typeName = ColumnAdapterNotes.deserialize(column.getMetadata().getAdapterNotes(), column.getMetadata().getName()).getTypeName();
                return TYPE_NAMES_REQUIRING_CAST.contains(typeName);
            }
            return false;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    };
}
