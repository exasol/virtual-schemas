package com.exasol.adapter.dialects.oracle;

import java.util.*;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.ColumnAdapterNotes;
import com.exasol.adapter.metadata.*;
import com.exasol.adapter.sql.*;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

public class OracleSqlGenerationVisitor extends SqlGenerationVisitor {

    public OracleSqlGenerationVisitor(final SqlDialect dialect, final SqlGenerationContext context) {
        super(dialect, context);

        this.aggregateFunctionsCast.add(AggregateFunction.SUM);
        this.aggregateFunctionsCast.add(AggregateFunction.MIN);
        this.aggregateFunctionsCast.add(AggregateFunction.MAX);
        this.aggregateFunctionsCast.add(AggregateFunction.AVG);
        this.aggregateFunctionsCast.add(AggregateFunction.MEDIAN);
        this.aggregateFunctionsCast.add(AggregateFunction.FIRST_VALUE);
        this.aggregateFunctionsCast.add(AggregateFunction.LAST_VALUE);
        this.aggregateFunctionsCast.add(AggregateFunction.STDDEV);
        this.aggregateFunctionsCast.add(AggregateFunction.STDDEV_POP);
        this.aggregateFunctionsCast.add(AggregateFunction.STDDEV_SAMP);
        this.aggregateFunctionsCast.add(AggregateFunction.VARIANCE);
        this.aggregateFunctionsCast.add(AggregateFunction.VAR_POP);
        this.aggregateFunctionsCast.add(AggregateFunction.VAR_SAMP);

        this.scalarFunctionsCast.add(ScalarFunction.ADD);
        this.scalarFunctionsCast.add(ScalarFunction.SUB);
        this.scalarFunctionsCast.add(ScalarFunction.MULT);
        this.scalarFunctionsCast.add(ScalarFunction.FLOAT_DIV);
        this.scalarFunctionsCast.add(ScalarFunction.NEG);
        this.scalarFunctionsCast.add(ScalarFunction.ABS);
        this.scalarFunctionsCast.add(ScalarFunction.ACOS);
        this.scalarFunctionsCast.add(ScalarFunction.ASIN);
        this.scalarFunctionsCast.add(ScalarFunction.ATAN);
        this.scalarFunctionsCast.add(ScalarFunction.ATAN2);
        this.scalarFunctionsCast.add(ScalarFunction.COS);
        this.scalarFunctionsCast.add(ScalarFunction.COSH);
        this.scalarFunctionsCast.add(ScalarFunction.COT);
        this.scalarFunctionsCast.add(ScalarFunction.DEGREES);
        this.scalarFunctionsCast.add(ScalarFunction.EXP);
        this.scalarFunctionsCast.add(ScalarFunction.GREATEST);
        this.scalarFunctionsCast.add(ScalarFunction.LEAST);
        this.scalarFunctionsCast.add(ScalarFunction.LN);
        this.scalarFunctionsCast.add(ScalarFunction.LOG);
        this.scalarFunctionsCast.add(ScalarFunction.MOD);
        this.scalarFunctionsCast.add(ScalarFunction.POWER);
        this.scalarFunctionsCast.add(ScalarFunction.RADIANS);
        this.scalarFunctionsCast.add(ScalarFunction.SIN);
        this.scalarFunctionsCast.add(ScalarFunction.SINH);
        this.scalarFunctionsCast.add(ScalarFunction.SQRT);
        this.scalarFunctionsCast.add(ScalarFunction.TAN);
        this.scalarFunctionsCast.add(ScalarFunction.TANH);
    }

    // If set to true, the selectlist elements will get aliases such as c1, c2, ...
    // Can be refactored if we find a better way to implement it
    private boolean requiresSelectListAliasesForLimit = false;

    private final Set<AggregateFunction> aggregateFunctionsCast = new HashSet<>();
    private final Set<ScalarFunction> scalarFunctionsCast = new HashSet<>();

    /**
     * ORACLE Syntax (before 12c) for LIMIT 10:</br>
     * SELECT LIMIT_SUBSELECT.* FROM ( <query-with-aliases> ) LIMIT_SUBSELECT WHERE ROWNUM <= 30
     *
     * ORACLE Syntax (before 12c) for LIMIT 10 OFFSET 20:</br>
     * SELECT c1, c2, ... FROM ( SELECT LIMIT_SUBSELECT.*, ROWNUM ROWNUM_SUB FROM ( <query-with-aliases> )
     * LIMIT_SUBSELECT WHERE ROWNUM <= 30 ) WHERE ROWNUM_SUB > 20
     *
     * The rownum filter is evaluated before ORDER BY, which is why we need subselects
     */
    @Override
    public String visit(final SqlStatementSelect select) throws AdapterException {
        if (!select.hasLimit()) {
            return super.visit(select);
        } else {
            final SqlLimit limit = select.getLimit();
            final StringBuilder builder = new StringBuilder();

            if (limit.hasOffset()) {
                // We cannot simply select * because this includes the rownum column. So we need aliases for select list
                // elements.
                builder.append("SELECT ");
                if (select.getSelectList().isRequestAnyColumn()) {
                    // The system requested any column
                    return "1";
                } else if (select.getSelectList().isSelectStar()) {
                    int numberOfColumns = 0;
                    final List<TableMetadata> tableMetadata = new ArrayList<>();
                    SqlGenerationHelper.getMetadataFrom(select.getFromClause(), tableMetadata);
                    for (final TableMetadata tableMeta : tableMetadata) {
                        numberOfColumns += tableMeta.getColumns().size();
                    }
                    builder.append(Joiner.on(", ").join(buildAliases(numberOfColumns)));
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

    private List<String> buildAliases(final int numSelectListElements) {
        final List<String> aliases = new ArrayList<>();
        for (int i = 0; i < numSelectListElements; i++) {
            aliases.add("c" + i);
        }
        return aliases;
    }

    @Override
    public String visit(final SqlSelectList selectList) throws AdapterException {
        if (selectList.isRequestAnyColumn()) {
            // The system requested any column
            return "1";
        }
        final List<String> selectListElements = new ArrayList<>();
        if (selectList.isSelectStar()) {
            // Do as if the user has all columns in select list
            final SqlStatementSelect select = (SqlStatementSelect) selectList.getParent();
            final boolean selectListRequiresCasts = isSelectListRequiresCasts(selectList, selectListElements, select);
            if (!this.requiresSelectListAliasesForLimit && !selectListRequiresCasts) {
                selectListElements.clear();
                selectListElements.add("*");
            }
        } else {
            for (final SqlNode node : selectList.getExpressions()) {
                selectListElements.add(node.accept(this));
            }
        }
        if (this.requiresSelectListAliasesForLimit) {
            // Add aliases to select list elements
            for (int i = 0; i < selectListElements.size(); i++) {
                selectListElements.set(i, selectListElements.get(i) + " AS c" + i);
            }
        }
        return Joiner.on(", ").join(selectListElements);
    }

    private boolean isSelectListRequiresCasts(final SqlSelectList selectList, final List<String> selectListElements,
            final SqlStatementSelect select) throws AdapterException {
        boolean selectListRequiresCasts = false;
        int columnId = 0;
        final List<TableMetadata> tableMetadata = new ArrayList<>();
        SqlGenerationHelper.getMetadataFrom(select.getFromClause(), tableMetadata);
        for (final TableMetadata tableMeta : tableMetadata) {
            for (final ColumnMetadata columnMeta : tableMeta.getColumns()) {
                final SqlColumn sqlColumn = new SqlColumn(columnId, columnMeta);
                sqlColumn.setParent(selectList);
                selectListRequiresCasts |= nodeRequiresCast(sqlColumn);
                selectListElements.add(sqlColumn.accept(this));
                ++columnId;
            }
        }
        return selectListRequiresCasts;
    }

    @Override
    public String visit(final SqlLimit limit) {
        // Limit is realized via a rownum filter in Oracle (< 12c)
        // Oracle 12c introduced nice syntax for limit and offset functionality: "OFFSET 4 ROWS FETCH NEXT 4 ROWS ONLY".
        // Nice to have to add this.
        return "";
    }

    @Override
    public String visit(final SqlPredicateLikeRegexp predicate) throws AdapterException {
        return "REGEXP_LIKE(" + predicate.getLeft().accept(this) + ", " + predicate.getPattern().accept(this) + ")";
    }

    @Override
    public String visit(final SqlColumn column) throws AdapterException {
        return getColumnProjectionString(column, super.visit(column));
    }

    @Override
    public String visit(final SqlLiteralExactnumeric literal) {
        String literalString = literal.getValue().toString();
        final boolean isDirectlyInSelectList = (literal.hasParent()
                && (literal.getParent().getType() == SqlNodeType.SELECT_LIST));
        if (isDirectlyInSelectList) {
            literalString = "TO_CHAR(" + literalString + ")";
        }
        return literalString;
    }

    @Override
    public String visit(final SqlLiteralDouble literal) {
        String literalString = Double.toString(literal.getValue());
        final boolean isDirectlyInSelectList = (literal.hasParent()
                && (literal.getParent().getType() == SqlNodeType.SELECT_LIST));
        if (isDirectlyInSelectList) {
            literalString = "TO_CHAR(" + literalString + ")";
        }
        return literalString;
    }

    @Override
    public String visit(final SqlFunctionAggregateGroupConcat function) throws AdapterException {
        final StringBuilder builder = new StringBuilder();
        builder.append("LISTAGG");
        builder.append("(");
        assert (function.getArguments() != null);
        assert ((function.getArguments().size() == 1) && (function.getArguments().get(0) != null));
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
    public String visit(final SqlFunctionAggregate function) throws AdapterException {
        String sql = super.visit(function);
        final boolean isDirectlyInSelectList = (function.hasParent()
                && (function.getParent().getType() == SqlNodeType.SELECT_LIST));
        if (isDirectlyInSelectList && this.aggregateFunctionsCast.contains(function.getFunction())) {
            // Cast to FLOAT because result set metadata has precision = 0, scale = 0
            sql = "CAST(" + sql + " AS FLOAT)";
        }
        return sql;
    }

    @Override
    public String visit(final SqlFunctionScalar function) throws AdapterException {
        String sql = super.visit(function);
        switch (function.getFunction()) {
        case LOCATE: {
            final List<String> argumentsSql = new ArrayList<>();
            for (final SqlNode node : function.getArguments()) {
                argumentsSql.add(node.accept(this));
            }
            final StringBuilder builder = new StringBuilder();
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
            for (final SqlNode node : function.getArguments()) {
                argumentsSql.add(node.accept(this));
            }
            final StringBuilder builder = new StringBuilder();
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
            builder.append("NVL(");
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
            builder.append(" / ");
            builder.append(argumentsSql.get(1));
            builder.append(") AS NUMBER(36, 0))");
            sql = builder.toString();
            break;
        }
        case COT: {
            final List<String> argumentsSql = new ArrayList<>();
            for (final SqlNode node : function.getArguments()) {
                argumentsSql.add(node.accept(this));
            }
            final StringBuilder builder = new StringBuilder();
            builder.append("(1 / TAN(");
            builder.append(argumentsSql.get(0));
            builder.append("))");
            sql = builder.toString();
            break;
        }
        case DEGREES: {
            final List<String> argumentsSql = new ArrayList<>();
            for (final SqlNode node : function.getArguments()) {
                argumentsSql.add(node.accept(this));
            }
            final StringBuilder builder = new StringBuilder();
            builder.append("((");
            builder.append(argumentsSql.get(0));
            // ACOS(-1) = PI
            builder.append(") * 180 / ACOS(-1))");
            sql = builder.toString();
            break;
        }
        case RADIANS: {
            final List<String> argumentsSql = new ArrayList<>();
            for (final SqlNode node : function.getArguments()) {
                argumentsSql.add(node.accept(this));
            }
            final StringBuilder builder = new StringBuilder();
            builder.append("((");
            builder.append(argumentsSql.get(0));
            // ACOS(-1) = PI
            builder.append(") * ACOS(-1) / 180)");
            sql = builder.toString();
            break;
        }
        case REPEAT: {
            final List<String> argumentsSql = new ArrayList<>();
            for (final SqlNode node : function.getArguments()) {
                argumentsSql.add(node.accept(this));
            }
            final StringBuilder builder = new StringBuilder();
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
            final List<String> argumentsSql = new ArrayList<>();
            for (final SqlNode node : function.getArguments()) {
                argumentsSql.add(node.accept(this));
            }
            final StringBuilder builder = new StringBuilder();
            builder.append("REVERSE(TO_CHAR(");
            builder.append(argumentsSql.get(0));
            builder.append("))");
            sql = builder.toString();
            break;
        }
        default:
            break;
        }

        final boolean isDirectlyInSelectList = (function.hasParent()
                && (function.getParent().getType() == SqlNodeType.SELECT_LIST));
        if (isDirectlyInSelectList && this.scalarFunctionsCast.contains(function.getFunction())) {
            // Cast to FLOAT because result set metadata has precision = 0, scale = 0
            sql = "CAST(" + sql + " AS FLOAT)";
        }

        return sql;
    }

    private String getColumnProjectionString(final SqlColumn column, String projString) throws AdapterException {
        final boolean isDirectlyInSelectList = (column.hasParent()
                && (column.getParent().getType() == SqlNodeType.SELECT_LIST));
        if (!isDirectlyInSelectList) {
            return projString;
        }
        final AbstractSqlDialect dialect = (AbstractSqlDialect) getDialect();
        final String typeName = ColumnAdapterNotes
                .deserialize(column.getMetadata().getAdapterNotes(), column.getMetadata().getName()).getTypeName();
        if ((typeName.startsWith("TIMESTAMP") && (((OracleSqlDialect) dialect).getImportType() == ImportType.JDBC))
                || typeName.startsWith("INTERVAL") || typeName.equals("BINARY_FLOAT")
                || typeName.equals("BINARY_DOUBLE") || typeName.equals("CLOB") || typeName.equals("NCLOB")) {
            projString = "TO_CHAR(" + projString + ")";
        } else if (typeName.equals("NUMBER")) {
            if (column.getMetadata().getType().getExaDataType() == DataType.ExaDataType.VARCHAR) {
                projString = "TO_CHAR(" + projString + ")";
            } else {
                if (needToCastNumberToDecimal(column)) {
                    final DataType castNumberToDecimalType = ((OracleSqlDialect) dialect).getOracleNumberTargetType();
                    projString = "CAST(" + projString + " AS DECIMAL(" + castNumberToDecimalType.getPrecision() + ","
                            + castNumberToDecimalType.getScale() + "))";
                }
            }
        } else if (typeName.equals("ROWID") || typeName.equals("UROWID")) {
            projString = "ROWIDTOCHAR(" + projString + ")";
        } else if (typeName.equals("BLOB")) {
            projString = "UTL_RAW.CAST_TO_VARCHAR2(" + projString + ")";
        }
        return projString;
    }

    private static final List<String> TYPE_NAMES_REQUIRING_CAST = ImmutableList.of("TIMESTAMP", "INTERVAL",
            "BINARY_FLOAT", "BINARY_DOUBLE", "CLOB", "NCLOB", "ROWID", "UROWID", "BLOB");

    private boolean nodeRequiresCast(final SqlNode node) throws AdapterException {
        if (node.getType() == SqlNodeType.COLUMN) {
            final SqlColumn column = (SqlColumn) node;
            final String typeName = ColumnAdapterNotes
                    .deserialize(column.getMetadata().getAdapterNotes(), column.getMetadata().getName()).getTypeName();
            if (typeName.equals("NUMBER")) {
                if (column.getMetadata().getType().getExaDataType() == DataType.ExaDataType.VARCHAR) {
                    return true;
                } else {
                    return needToCastNumberToDecimal(column);
                }
            } else {
                for (final String typeRequiringCast : TYPE_NAMES_REQUIRING_CAST) {
                    if (typeName.startsWith(typeRequiringCast)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * This method determines if a NUMBER column needs to be casted to the DECIMAL type specified in the
     * oracle_cast_number_to_decimal_with_precision_and_scale property. This is done by checking if the target type is
     * the type specified in the property, assuming that this type was set according to the property. This method is not
     * exact and will also add CASTs to columns that have the exact same type as specified in the property.
     *
     * @param column a NUMBER column
     * @return true if a cast is necessary for the NUMBER column
     */
    private boolean needToCastNumberToDecimal(final SqlColumn column) {
        final AbstractSqlDialect dialect = (AbstractSqlDialect) getDialect();
        final DataType columnType = column.getMetadata().getType();
        final DataType castNumberToDecimalType = ((OracleSqlDialect) dialect).getOracleNumberTargetType();
        return (columnType.getPrecision() == castNumberToDecimalType.getPrecision())
                && (columnType.getScale() == castNumberToDecimalType.getScale());
    }
}
