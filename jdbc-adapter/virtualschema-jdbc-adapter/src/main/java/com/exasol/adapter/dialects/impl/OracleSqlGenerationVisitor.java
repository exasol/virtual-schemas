package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlGenerationContext;
import com.exasol.adapter.dialects.SqlGenerationVisitor;
import com.exasol.adapter.jdbc.ColumnAdapterNotes;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.sql.*;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class OracleSqlGenerationVisitor extends SqlGenerationVisitor {

    public OracleSqlGenerationVisitor(SqlDialect dialect, SqlGenerationContext context) {
        super(dialect, context);

        if (dialect instanceof OracleSqlDialect &&
                ((OracleSqlDialect)dialect).getCastAggFuncToFloat()) {
            aggregateFunctionsCast.add(AggregateFunction.SUM);
            aggregateFunctionsCast.add(AggregateFunction.MIN);
            aggregateFunctionsCast.add(AggregateFunction.MAX);
            aggregateFunctionsCast.add(AggregateFunction.AVG);
            aggregateFunctionsCast.add(AggregateFunction.MEDIAN);
            aggregateFunctionsCast.add(AggregateFunction.FIRST_VALUE);
            aggregateFunctionsCast.add(AggregateFunction.LAST_VALUE);
            aggregateFunctionsCast.add(AggregateFunction.STDDEV);
            aggregateFunctionsCast.add(AggregateFunction.STDDEV_POP);
            aggregateFunctionsCast.add(AggregateFunction.STDDEV_SAMP);
            aggregateFunctionsCast.add(AggregateFunction.VARIANCE);
            aggregateFunctionsCast.add(AggregateFunction.VAR_POP);
            aggregateFunctionsCast.add(AggregateFunction.VAR_SAMP);
        }

        if (dialect instanceof OracleSqlDialect &&
                ((OracleSqlDialect)dialect).getCastScalarFuncToFloat()) {
            scalarFunctionsCast.add(ScalarFunction.ADD);
            scalarFunctionsCast.add(ScalarFunction.SUB);
            scalarFunctionsCast.add(ScalarFunction.MULT);
            scalarFunctionsCast.add(ScalarFunction.FLOAT_DIV);
            scalarFunctionsCast.add(ScalarFunction.NEG);
            scalarFunctionsCast.add(ScalarFunction.ABS);
            scalarFunctionsCast.add(ScalarFunction.ACOS);
            scalarFunctionsCast.add(ScalarFunction.ASIN);
            scalarFunctionsCast.add(ScalarFunction.ATAN);
            scalarFunctionsCast.add(ScalarFunction.ATAN2);
            scalarFunctionsCast.add(ScalarFunction.COS);
            scalarFunctionsCast.add(ScalarFunction.COSH);
            scalarFunctionsCast.add(ScalarFunction.COT);
            scalarFunctionsCast.add(ScalarFunction.DEGREES);
            scalarFunctionsCast.add(ScalarFunction.EXP);
            scalarFunctionsCast.add(ScalarFunction.GREATEST);
            scalarFunctionsCast.add(ScalarFunction.LEAST);
            scalarFunctionsCast.add(ScalarFunction.LN);
            scalarFunctionsCast.add(ScalarFunction.LOG);
            scalarFunctionsCast.add(ScalarFunction.MOD);
            scalarFunctionsCast.add(ScalarFunction.POWER);
            scalarFunctionsCast.add(ScalarFunction.RADIANS);
            scalarFunctionsCast.add(ScalarFunction.SIN);
            scalarFunctionsCast.add(ScalarFunction.SINH);
            scalarFunctionsCast.add(ScalarFunction.SQRT);
            scalarFunctionsCast.add(ScalarFunction.TAN);
            scalarFunctionsCast.add(ScalarFunction.TANH);
        }
    }

    // If set to true, the selectlist elements will get aliases such as c1, c2, ...
    // Can be refactored if we find a better way to implement it
    private boolean requiresSelectListAliasesForLimit = false;

    private Set<AggregateFunction> aggregateFunctionsCast = new HashSet<>();
    private Set<ScalarFunction> scalarFunctionsCast = new HashSet<>();

    /**
     * ORACLE Syntax (before 12c) for LIMIT 10:</br>
     * SELECT LIMIT_SUBSELECT.* FROM
     * (
     *  <query-with-aliases>
     * )
     * LIMIT_SUBSELECT WHERE ROWNUM <= 30
     *
     * ORACLE Syntax (before 12c) for LIMIT 10 OFFSET 20:</br>
     * SELECT c1, c2, ... FROM
     * (
     *  SELECT LIMIT_SUBSELECT.*, ROWNUM ROWNUM_SUB FROM
     *  (
     *   <query-with-aliases>
     *  )
     *  LIMIT_SUBSELECT WHERE ROWNUM <= 30
     * ) WHERE ROWNUM_SUB > 20
     *
     * The rownum filter is evaluated before ORDER BY, which is why we need subselects
     */
    @Override
    public String visit(SqlStatementSelect select) throws AdapterException {
        if (!select.hasLimit()) {
            return super.visit(select);
        } else {
            SqlLimit limit = select.getLimit();
            StringBuilder builder = new StringBuilder();

            if (limit.hasOffset()) {
                // We cannot simply select * because this includes the rownum column. So we need aliases for select list elements.
                builder.append("SELECT ");
                if (select.getSelectList().isRequestAnyColumn()) {
                    // The system requested any column
                    return "true";
                } else if (select.getSelectList().isSelectStar()) {
                    builder.append(Joiner.on(", ").join(buildAliases(select.getFromClause().getMetadata().getColumns().size())));
                } else {
                    builder.append(Joiner.on(", ").join(buildAliases(select.getSelectList().getExpressions().size())));
                }
                builder.append(" FROM ( ");
                builder.append("SELECT LIMIT_SUBSELECT.*, ROWNUM ROWNUM_SUB FROM ( ");
                this.requiresSelectListAliasesForLimit = true;
                builder.append(super.visit(select));
                builder.append(" ) LIMIT_SUBSELECT WHERE ROWNUM <= " + (limit.getLimit() + limit.getOffset()));
                builder.append(" ) WHERE ROWNUM_SUB > " + limit.getOffset());
            } else {
                builder.append("SELECT LIMIT_SUBSELECT.* FROM ( ");
                builder.append(super.visit(select));
                builder.append(" ) LIMIT_SUBSELECT WHERE ROWNUM <= " + (limit.getLimit() + limit.getOffset()));
            }
            return builder.toString();
        }
    }

    private List<String> buildAliases(int numSelectListElements) {
        List<String> aliases = new ArrayList<>();
        for (int i=0; i<numSelectListElements; i++) {
            aliases.add("c" + i);
        }
        return aliases;
    }

    @Override
    public String visit(SqlSelectList selectList) throws AdapterException {
        if (selectList.isRequestAnyColumn()) {
            // The system requested any column
            return "true";
        }
        List<String> selectListElements = new ArrayList<>();
        if (selectList.isSelectStar()) {
            // Do as if the user has all columns in select list
            SqlStatementSelect select = (SqlStatementSelect) selectList.getParent();
            boolean selectListRequiresCasts = false;
            int columnId = 0;
            for (ColumnMetadata columnMeta : select.getFromClause().getMetadata().getColumns()) {
                SqlColumn sqlColumn = new SqlColumn(columnId, columnMeta);
                sqlColumn.setParent(selectList);
                selectListRequiresCasts |= nodeRequiresCast(sqlColumn);
                selectListElements.add(sqlColumn.accept(this));
                ++columnId;
            }
            if (!requiresSelectListAliasesForLimit && !selectListRequiresCasts) {
                selectListElements.clear();
                selectListElements.add("*");
            }
        } else {
            for (SqlNode node : selectList.getExpressions()) {
                selectListElements.add(node.accept(this));
            }
        }
        if (requiresSelectListAliasesForLimit) {
            // Add aliases to select list elements
            for (int i=0; i<selectListElements.size(); i++) {
                selectListElements.set(i, selectListElements.get(i) + " AS c" + i);
            }
        }
        return Joiner.on(", ").join(selectListElements);
    }

    @Override
    public String visit(SqlLimit limit) {
        // Limit is realized via a rownum filter in Oracle (< 12c)
        // Oracle 12c introduced nice syntax for limit and offset functionality: "OFFSET 4 ROWS FETCH NEXT 4 ROWS ONLY". Nice to have to add this.
        return "";
    }

    @Override
    public String visit(SqlPredicateLikeRegexp predicate) throws AdapterException {
        return "REGEXP_LIKE(" + predicate.getLeft().accept(this) + ", "
                + predicate.getPattern().accept(this) + ")";
    }

    @Override
    public String visit(SqlColumn column) throws AdapterException {
        return getColumnProjectionString(column, super.visit(column));
    }

    @Override
    public String visit(SqlLiteralExactnumeric literal) {
        String literalString = literal.getValue().toString();
        boolean isDirectlyInSelectList = (literal.hasParent() && literal.getParent().getType() == SqlNodeType.SELECT_LIST);
        if (isDirectlyInSelectList) {
            literalString = "TO_CHAR(" + literalString + ")";
        }
        return literalString;
    }

    @Override
    public String visit(SqlLiteralDouble literal) {
        String literalString = Double.toString(literal.getValue());
        boolean isDirectlyInSelectList = (literal.hasParent() && literal.getParent().getType() == SqlNodeType.SELECT_LIST);
        if (isDirectlyInSelectList) {
            literalString = "TO_CHAR(" + literalString + ")";
        }
        return literalString;
    }

    @Override
    public String visit(SqlFunctionAggregateGroupConcat function) throws AdapterException {
        StringBuilder builder = new StringBuilder();
        builder.append("LISTAGG");
        builder.append("(");
        assert(function.getArguments() != null);
        assert(function.getArguments().size() == 1 && function.getArguments().get(0) != null);
        String expression = function.getArguments().get(0).accept(this);
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
                if (!function.getOrderBy().nullsLast().get(i)) {
                    builder.append(" NULLS FIRST");
                }
            }
        } else {
            builder.append(expression);
        }
        builder.append(")");
        return builder.toString();
    }

    @Override
    public String visit(SqlFunctionAggregate function) throws AdapterException {
        String sql = super.visit(function);
        boolean isDirectlyInSelectList = (function.hasParent() && function.getParent().getType() == SqlNodeType.SELECT_LIST);
        if (isDirectlyInSelectList && aggregateFunctionsCast.contains(function.getFunction())) {
            // Cast to FLOAT because result set metadata has precision = 0, scale = 0
            sql = "CAST("  + sql + " AS FLOAT)";
        }
        return sql;
    }

    @Override
    public String visit(SqlFunctionScalar function) throws AdapterException {
        String sql = super.visit(function);
        switch (function.getFunction()) {
        case LOCATE: {
            List<String> argumentsSql = new ArrayList<>();
            for (SqlNode node : function.getArguments()) {
                argumentsSql.add(node.accept(this));
            }
            StringBuilder builder = new StringBuilder();
            builder.append("INSTR(");
            builder.append(argumentsSql.get(1));
            builder.append(", ");
            builder.append(argumentsSql.get(0));
            if (argumentsSql.size() > 2) {
                builder.append(", ");
                builder.append(argumentsSql.get(2));
            }
            builder.append(")");
            sql = builder.toString();
            break;
        }
        case TRIM: {
            List<String> argumentsSql = new ArrayList<>();
            for (SqlNode node : function.getArguments()) {
                argumentsSql.add(node.accept(this));
            }
            StringBuilder builder = new StringBuilder();
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
            List<String> argumentsSql = new ArrayList<>();
            for (SqlNode node : function.getArguments()) {
                argumentsSql.add(node.accept(this));
            }
            StringBuilder builder = new StringBuilder();
            builder.append("(");
            builder.append(argumentsSql.get(0));
            builder.append(" + INTERVAL '");
            if (function.getFunction() == ScalarFunction.ADD_WEEKS) {
                builder.append(7 * Integer.parseInt(argumentsSql.get(1)));
            } else {
                builder.append(argumentsSql.get(1));
            }
            builder.append("' ");
            switch (function.getFunction()) {
            case ADD_DAYS:
            case ADD_WEEKS:
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
            case ADD_YEARS:
                builder.append("YEAR");
                break;
            default:
                break;
            }
            builder.append(")");
            sql = builder.toString();
            break;
        }
        case CURRENT_DATE:
            sql = "CURRENT_DATE";
            break;
        case CURRENT_TIMESTAMP:
            sql = "CURRENT_TIMESTAMP";
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
            sql = "TO_DATE(SYSDATE)";
            break;
        case SYSTIMESTAMP:
            sql = "SYSTIMESTAMP";
            break;
        case BIT_AND:
            sql = sql.replaceFirst("^BIT_AND", "BITAND");
            break;
        case BIT_TO_NUM:
            sql = sql.replaceFirst("^BIT_TO_NUM", "BIN_TO_NUM");
            break;
        case NULLIFZERO: {
            List<String> argumentsSql = new ArrayList<>();
            for (SqlNode node : function.getArguments()) {
                argumentsSql.add(node.accept(this));
            }
            StringBuilder builder = new StringBuilder();
            builder.append("NULLIF(");
            builder.append(argumentsSql.get(0));
            builder.append(", 0)");
            sql = builder.toString();
            break;
        }
        case ZEROIFNULL: {
            List<String> argumentsSql = new ArrayList<>();
            for (SqlNode node : function.getArguments()) {
                argumentsSql.add(node.accept(this));
            }
            StringBuilder builder = new StringBuilder();
            builder.append("NVL(");
            builder.append(argumentsSql.get(0));
            builder.append(", 0)");
            sql = builder.toString();
            break;
        }
        case DIV: {
            List<String> argumentsSql = new ArrayList<>();
            for (SqlNode node : function.getArguments()) {
                argumentsSql.add(node.accept(this));
            }
            StringBuilder builder = new StringBuilder();
            builder.append("CAST(FLOOR(");
            builder.append(argumentsSql.get(0));
            builder.append(" / ");
            builder.append(argumentsSql.get(1));
            builder.append(") AS NUMBER(36, 0))");
            sql = builder.toString();
            break;
        }
        case COT: {
            List<String> argumentsSql = new ArrayList<>();
            for (SqlNode node : function.getArguments()) {
                argumentsSql.add(node.accept(this));
            }
            StringBuilder builder = new StringBuilder();
            builder.append("(1 / TAN(");
            builder.append(argumentsSql.get(0));
            builder.append("))");
            sql = builder.toString();
            break;
        }
        case DEGREES: {
            List<String> argumentsSql = new ArrayList<>();
            for (SqlNode node : function.getArguments()) {
                argumentsSql.add(node.accept(this));
            }
            StringBuilder builder = new StringBuilder();
            builder.append("((");
            builder.append(argumentsSql.get(0));
            // ACOS(-1) = PI
            builder.append(") * 180 / ACOS(-1))");
            sql = builder.toString();
            break;
        }
        case RADIANS: {
            List<String> argumentsSql = new ArrayList<>();
            for (SqlNode node : function.getArguments()) {
                argumentsSql.add(node.accept(this));
            }
            StringBuilder builder = new StringBuilder();
            builder.append("((");
            builder.append(argumentsSql.get(0));
            // ACOS(-1) = PI
            builder.append(") * ACOS(-1) / 180)");
            sql = builder.toString();
            break;
        }
        case REPEAT: {
            List<String> argumentsSql = new ArrayList<>();
            for (SqlNode node : function.getArguments()) {
                argumentsSql.add(node.accept(this));
            }
            StringBuilder builder = new StringBuilder();
            builder.append("RPAD(TO_CHAR(");
            builder.append(argumentsSql.get(0));
            builder.append("), LENGTH(");
            builder.append(argumentsSql.get(0));
            builder.append(") * ROUND(");
            builder.append(argumentsSql.get(1));
            builder.append("), ");
            builder.append(argumentsSql.get(0));
            builder.append(")");
            sql = builder.toString();
            break;
        }
        case REVERSE: {
            List<String> argumentsSql = new ArrayList<>();
            for (SqlNode node : function.getArguments()) {
                argumentsSql.add(node.accept(this));
            }
            StringBuilder builder = new StringBuilder();
            builder.append("REVERSE(TO_CHAR(");
            builder.append(argumentsSql.get(0));
            builder.append("))");
            sql = builder.toString();
            break;
        }
        default:
            break;
        }

        boolean isDirectlyInSelectList = (function.hasParent() && function.getParent().getType() == SqlNodeType.SELECT_LIST);
        if (isDirectlyInSelectList && scalarFunctionsCast.contains(function.getFunction())) {
            // Cast to FLOAT because result set metadata has precision = 0, scale = 0
            sql = "CAST("  + sql + " AS FLOAT)";
        }

        return sql;
    }

    private String getColumnProjectionString(SqlColumn column, String projString) throws AdapterException {
        boolean isDirectlyInSelectList = (column.hasParent() && column.getParent().getType() == SqlNodeType.SELECT_LIST);
        if (!isDirectlyInSelectList) {
            return projString;
        }
        String typeName = ColumnAdapterNotes.deserialize(column.getMetadata().getAdapterNotes(), column.getMetadata().getName()).getTypeName();
        if (typeName.startsWith("TIMESTAMP") ||
            typeName.startsWith("INTERVAL") ||
            typeName.equals("BINARY_FLOAT") ||
            typeName.equals("BINARY_DOUBLE") ||
            typeName.equals("CLOB") ||
            typeName.equals("NCLOB")) {
            projString = "TO_CHAR(" + projString + ")";
        } else if (typeName.equals("NUMBER") &&
                   column.getMetadata().getType().getExaDataType() == DataType.ExaDataType.VARCHAR) {
            projString = "TO_CHAR(" + projString + ")";
        } else if (typeName.equals("ROWID") ||
                   typeName.equals("UROWID")) {
            projString = "ROWIDTOCHAR(" + projString + ")";
        } else if (typeName.equals("BLOB")) {
            projString = "UTL_RAW.CAST_TO_VARCHAR2(" + projString + ")";
        }
        return projString;
    }

    private static final List<String> TYPE_NAMES_REQUIRING_CAST = ImmutableList.of("TIMESTAMP","INTERVAL","BINARY_FLOAT","BINARY_DOUBLE","CLOB","NCLOB","ROWID", "UROWID", "BLOB");

    private boolean nodeRequiresCast(SqlNode node) throws AdapterException {
        if (node.getType() == SqlNodeType.COLUMN) {
            SqlColumn column = (SqlColumn)node;
            String typeName = ColumnAdapterNotes.deserialize(column.getMetadata().getAdapterNotes(), column.getMetadata().getName()).getTypeName();
            if (typeName.equals("NUMBER") && column.getMetadata().getType().getExaDataType() == DataType.ExaDataType.VARCHAR) {
                return true;
            } else {
                return TYPE_NAMES_REQUIRING_CAST.contains(typeName);
            }
        }
        return false;
    }
}
