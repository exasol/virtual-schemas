package com.exasol.adapter.dialects.oracle;

import static com.exasol.adapter.sql.AggregateFunction.*;
import static com.exasol.adapter.sql.ScalarFunction.*;

import java.util.*;
import java.util.logging.Logger;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.ColumnAdapterNotes;
import com.exasol.adapter.metadata.*;
import com.exasol.adapter.sql.*;

/**
 * This class generates SQL queries for the {@link OracleSqlGenerationVisitor}.
 */
public class OracleSqlGenerationVisitor extends SqlGenerationVisitor {
    public static final Logger LOGGER = Logger.getLogger(OracleSqlGenerationVisitor.class.getName());
    private boolean requiresSelectListAliasesForLimit = false;
    private static final String TIMESTAMP_FORMAT = "'YYYY-MM-DD HH24:MI:SS.FF3'";
    private static final List<String> TYPE_NAMES_REQUIRING_CAST = List.of("TIMESTAMP", "INTERVAL", "BINARY_FLOAT",
            "BINARY_DOUBLE", "ROWID", "UROWID", "BLOB");
    private final Set<AggregateFunction> aggregateFunctionsCast = EnumSet.noneOf(AggregateFunction.class);
    private final Set<ScalarFunction> scalarFunctionsCast = EnumSet.noneOf(ScalarFunction.class);

    /**
     * Create a new instance of the {@link OracleSqlGenerationVisitor}.
     *
     * @param dialect {@link OracleSqlDialect} SQL dialect
     * @param context SQL generation context
     */
    public OracleSqlGenerationVisitor(final SqlDialect dialect, final SqlGenerationContext context) {
        super(dialect, context);
        addAggregateFunctions();
        addScalarFunctions();
    }

    private void addScalarFunctions() {
        this.scalarFunctionsCast.addAll(Arrays.asList(ADD, SUB, MULT, FLOAT_DIV, NEG, ABS, ACOS, ASIN, ATAN, ATAN2, COS,
                COSH, COT, DEGREES, EXP, GREATEST, LEAST, LN, LOG, MOD, POWER, RADIANS, SIN, SINH, SQRT, TAN, TANH));
    }

    private void addAggregateFunctions() {
        this.aggregateFunctionsCast.addAll(Arrays.asList(SUM, MIN, MAX, AVG, MEDIAN, FIRST_VALUE, LAST_VALUE, STDDEV,
                STDDEV_POP, STDDEV_SAMP, VARIANCE, VAR_POP, VAR_SAMP));

    }

    Set<AggregateFunction> getAggregateFunctionsCast() {
        return this.aggregateFunctionsCast;
    }

    Set<ScalarFunction> getScalarFunctionsCast() {
        return this.scalarFunctionsCast;
    }

    /**
     * ORACLE Syntax (before 12c) for LIMIT 10:<br>
     * SELECT LIMIT_SUBSELECT.* FROM ( &lt;query-with-aliases&gt; ) LIMIT_SUBSELECT WHERE ROWNUM &lt;= 30
     *
     * ORACLE Syntax (before 12c) for LIMIT 10 OFFSET 20:<br>
     * SELECT c1, c2, ... FROM ( SELECT LIMIT_SUBSELECT.*, ROWNUM ROWNUM_SUB FROM ( &lt;query-with-aliases&gt; )
     * LIMIT_SUBSELECT WHERE ROWNUM &lt;= 30 ) WHERE ROWNUM_SUB &gt; 20
     *
     * The ROWNUM filter is evaluated before ORDER BY, which is why we need sub-selects
     */
    @Override
    public String visit(final SqlStatementSelect select) throws AdapterException {
        if (!select.hasLimit()) {
            return super.visit(select);
        } else {
            return getSqlStatementSelectWithLimit(select);
        }
    }

    private String getSqlStatementSelectWithLimit(final SqlStatementSelect select) throws AdapterException {
        final SqlLimit limit = select.getLimit();
        if (limit.hasOffset()) {
            return getSqlStatementSelectWithOffset(select, limit);
        } else {
            return getSqlStatementSelectWithoutOffset(select, limit);
        }
    }

    private String getSqlStatementSelectWithOffset(final SqlStatementSelect select, final SqlLimit limit)
            throws AdapterException {
        final StringBuilder builder = new StringBuilder();
        builder.append("SELECT ");
        if (select.getSelectList().isRequestAnyColumn()) {
            return "1";
        } else if (select.getSelectList().isSelectStar()) {
            appendSelectStar(select, builder);
        } else {
            final int numberOfExpressions = select.getSelectList().getExpressions().size();
            builder.append(String.join(", ", buildAliases(numberOfExpressions)));
        }
        builder.append(" FROM ( ");
        builder.append("SELECT LIMIT_SUBSELECT.*, ROWNUM ROWNUM_SUB FROM ( ");
        this.requiresSelectListAliasesForLimit = true;
        builder.append(super.visit(select));
        builder.append(" ) LIMIT_SUBSELECT WHERE ROWNUM <= ");
        builder.append(limit.getLimit() + limit.getOffset());
        builder.append(" ) WHERE ROWNUM_SUB > ");
        builder.append(limit.getOffset());
        return builder.toString();
    }

    private void appendSelectStar(final SqlStatementSelect select, final StringBuilder builder) {
        int numberOfColumns = 0;
        final List<TableMetadata> tableMetadata = new ArrayList<>();
        SqlGenerationHelper.addMetadata(select.getFromClause(), tableMetadata);
        for (final TableMetadata tableMeta : tableMetadata) {
            numberOfColumns += tableMeta.getColumns().size();
        }
        builder.append(String.join(", ", buildAliases(numberOfColumns)));
    }

    private List<String> buildAliases(final int numSelectListElements) {
        final List<String> aliases = new ArrayList<>(numSelectListElements);
        for (int i = 0; i < numSelectListElements; i++) {
            aliases.add("c" + i);
        }
        return aliases;
    }

    private String getSqlStatementSelectWithoutOffset(final SqlStatementSelect select, final SqlLimit limit)
            throws AdapterException {
        final StringBuilder builder = new StringBuilder();
        builder.append("SELECT LIMIT_SUBSELECT.* FROM ( ");
        builder.append(super.visit(select));
        builder.append(" ) LIMIT_SUBSELECT WHERE ROWNUM <= ");
        builder.append(limit.getLimit() + limit.getOffset());
        return builder.toString();
    }

    @Override
    public String visit(final SqlSelectList selectList) throws AdapterException {
        if (selectList.isRequestAnyColumn()) {
            return "1";
        } else {
            return getSqlSelectList(selectList);
        }
    }

    private String getSqlSelectList(final SqlSelectList selectList) throws AdapterException {
        final List<String> selectListElements = new ArrayList<>();
        if (selectList.isSelectStar()) {
            getSelectStarList(selectList, selectListElements);
        } else {
            for (final SqlNode node : selectList.getExpressions()) {
                selectListElements.add(node.accept(this));
            }
        }
        if (this.requiresSelectListAliasesForLimit) {
            addColumnAliases(selectListElements);
        }
        return String.join(", ", selectListElements);
    }

    public void getSelectStarList(final SqlSelectList selectList, final List<String> selectListElements)
            throws AdapterException {
        final SqlStatementSelect select = (SqlStatementSelect) selectList.getParent();
        final boolean selectListRequiresCasts = isSelectListRequiresCasts(selectList, selectListElements, select);
        if (!this.requiresSelectListAliasesForLimit && !selectListRequiresCasts) {
            LOGGER.fine("Converting select list to SELECT *");
            selectListElements.clear();
            selectListElements.add("*");
        }
    }

    private boolean isSelectListRequiresCasts(final SqlSelectList selectList, final List<String> selectListElements,
            final SqlStatementSelect select) throws AdapterException {
        boolean selectListRequiresCasts = false;
        int columnId = 0;
        final List<TableMetadata> tableMetadata = new ArrayList<>();
        SqlGenerationHelper.addMetadata(select.getFromClause(), tableMetadata);
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

    private boolean nodeRequiresCast(final SqlNode node) throws AdapterException {
        if (node.getType() == SqlNodeType.COLUMN) {
            return checkIfColumnRequiresCast((SqlColumn) node);
        } else {
            return false;
        }
    }

    private boolean checkIfColumnRequiresCast(final SqlColumn node) throws AdapterException {
        final String typeName = ColumnAdapterNotes
                .deserialize(node.getMetadata().getAdapterNotes(), node.getMetadata().getName()).getTypeName();
        if (typeName.equals("NUMBER")) {
            if (node.getMetadata().getType().getExaDataType() == DataType.ExaDataType.VARCHAR) {
                return true;
            } else {
                return checkIfNeedToCastNumberToDecimal(node);
            }
        } else {
            for (final String typeRequiringCast : TYPE_NAMES_REQUIRING_CAST) {
                if (typeName.startsWith(typeRequiringCast)) {
                    return true;
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
    private boolean checkIfNeedToCastNumberToDecimal(final SqlColumn column) {
        final AbstractSqlDialect dialect = (AbstractSqlDialect) getDialect();
        final DataType columnType = column.getMetadata().getType();
        final DataType castNumberToDecimalType = ((OracleSqlDialect) dialect).getOracleNumberTargetType();
        return (columnType.getPrecision() == castNumberToDecimalType.getPrecision())
                && (columnType.getScale() == castNumberToDecimalType.getScale());
    }

    public void addColumnAliases(final List<String> selectListElements) {
        for (int i = 0; i < selectListElements.size(); i++) {
            selectListElements.set(i, selectListElements.get(i) + " AS c" + i);
        }
    }

    // Limit is realized via a {@code ROWNUM} filter in Oracle (< 12c) Oracle 12c introduced nice syntax for limit and
    // offset
    // functionality: "OFFSET 4 ROWS FETCH NEXT 4 ROWS ONLY"
    @Override
    public String visit(final SqlLimit limit) {
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

    private String getColumnProjectionString(final SqlColumn column, final String projectionString)
            throws AdapterException {
        final boolean isDirectlyInSelectList = (column.hasParent()
                && (column.getParent().getType() == SqlNodeType.SELECT_LIST));
        if (!isDirectlyInSelectList) {
            return projectionString;
        } else {
            return getProjectionString(column, projectionString);
        }
    }

    private String getProjectionString(final SqlColumn column, final String projectionString) throws AdapterException {
        final AbstractSqlDialect dialect = (AbstractSqlDialect) getDialect();
        final String typeName = ColumnAdapterNotes
                .deserialize(column.getMetadata().getAdapterNotes(), column.getMetadata().getName()).getTypeName();
        if (typeName.startsWith("INTERVAL") || typeName.equals("BINARY_FLOAT") || typeName.equals("BINARY_DOUBLE")) {
            return createToChar(projectionString);
        } else if (typeName.startsWith("TIMESTAMP")
                && (((OracleSqlDialect) dialect).getImportType() == ImportType.JDBC)) {
            return "TO_TIMESTAMP(TO_CHAR(" + projectionString + ", " + TIMESTAMP_FORMAT + "), " + TIMESTAMP_FORMAT
                    + ")";
        } else if (typeName.equals("NUMBER")) {
            return getNumberProjectionString(column, projectionString, (OracleSqlDialect) dialect);
        } else if (typeName.equals("ROWID") || typeName.equals("UROWID")) {
            return "ROWIDTOCHAR(" + projectionString + ")";
        } else if (typeName.equals("BLOB")) {
            return "UTL_RAW.CAST_TO_VARCHAR2(UTL_ENCODE.BASE64_DECODE(" + projectionString + "))";
        } else {
            return projectionString;
        }
    }

    public String createToChar(final String operand) {
        return "TO_CHAR(" + operand + ")";
    }

    private String getNumberProjectionString(final SqlColumn column, final String projectionString,
            final OracleSqlDialect dialect) {
        if (column.getMetadata().getType().getExaDataType() == DataType.ExaDataType.VARCHAR) {
            return createToChar(projectionString);
        } else {
            if (checkIfNeedToCastNumberToDecimal(column)) {
                final DataType castNumberToDecimalType = dialect.getOracleNumberTargetType();
                return cast(projectionString, "DECIMAL(" + castNumberToDecimalType.getPrecision() + ","
                        + castNumberToDecimalType.getScale() + ")");
            } else {
                return projectionString;
            }
        }
    }

    private String cast(final String value, final String as) {
        return "CAST(" + value + " AS " + as + ")";
    }

    @Override
    public String visit(final SqlLiteralExactnumeric literal) {
        final String literalString = literal.getValue().toString();
        return getLiteralString(literalString, literal.hasParent(), literal.getParent());
    }

    private String getLiteralString(final String literalString, final boolean b, final SqlNode parent) {
        final boolean isDirectlyInSelectList = (b && (parent.getType() == SqlNodeType.SELECT_LIST));
        if (isDirectlyInSelectList) {
            return createToChar(literalString);
        }
        return literalString;
    }

    @Override
    public String visit(final SqlLiteralDouble literal) {
        final String literalString = Double.toString(literal.getValue());
        return getLiteralString(literalString, literal.hasParent(), literal.getParent());
    }

    @Override
    public String visit(final SqlFunctionAggregateGroupConcat function) throws AdapterException {
        final StringBuilder builder = new StringBuilder();
        builder.append("LISTAGG");
        builder.append("(");
        if ((function.getArguments() != null) && (function.getArguments().size() == 1)
                && (function.getArguments().get(0) != null)) {
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
                builder.append(getOrderByString(function));
            } else {
                builder.append(expression);
            }
            builder.append(")");
            return builder.toString();
        } else {
            throw new SqlGenerationVisitorException(
                    "List of arguments of SqlFunctionAggregateGroupConcat should have one argument.");
        }
    }

    private String getOrderByString(final SqlFunctionAggregateGroupConcat function) throws AdapterException {
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < function.getOrderBy().getExpressions().size(); i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(function.getOrderBy().getExpressions().get(i).accept(this));
            if (function.getOrderBy().isAscending().get(i).equals(false)) {
                builder.append(" DESC");
            }
            if (function.getOrderBy().nullsLast().get(i).equals(false)) {
                builder.append(" NULLS FIRST");
            }
        }
        return builder.toString();
    }

    @Override
    public String visit(final SqlFunctionAggregate function) throws AdapterException {
        final boolean isDirectlyInSelectList = (function.hasParent()
                && (function.getParent().getType() == SqlNodeType.SELECT_LIST));
        if (isDirectlyInSelectList && this.aggregateFunctionsCast.contains(function.getFunction())) {
            // Cast to FLOAT because result set metadata has precision = 0, scale = 0
            return cast(super.visit(function), "FLOAT");
        }
        return super.visit(function);
    }

    @Override
    public String visit(final SqlFunctionScalar function) throws AdapterException {
        String sql = super.visit(function);
        switch (function.getFunction()) {
        case LOCATE:
            sql = getLocate(function);
            break;
        case TRIM:
            sql = getTrim(function);
            break;
        case ADD_DAYS:
        case ADD_HOURS:
        case ADD_MINUTES:
        case ADD_SECONDS:
        case ADD_WEEKS:
        case ADD_YEARS:
            sql = getTimeOrDate(function);
            break;
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
        case NULLIFZERO:
            sql = getSqlFunctionScalar(function, "NULLIF(", ", 0)");
            break;
        case ZEROIFNULL:
            sql = getSqlFunctionScalar(function, "NVL(", ", 0)");
            break;
        case DIV:
            sql = getDiv(function);
            break;
        case COT:
            sql = getSqlFunctionScalar(function, "(1 / TAN(", "))");
            break;
        case DEGREES:
            sql = getSqlFunctionScalar(function, "((", ") * 180 / ACOS(-1))");
            break;
        case RADIANS:
            sql = getSqlFunctionScalar(function, "((", ") * ACOS(-1) / 180)");
            break;
        case REPEAT:
            sql = getRepeat(function);
            break;
        case REVERSE:
            sql = getSqlFunctionScalar(function, "REVERSE(TO_CHAR(", "))");
            break;
        default:
            break;
        }
        final boolean isDirectlyInSelectList = (function.hasParent()
                && (function.getParent().getType() == SqlNodeType.SELECT_LIST));
        if (isDirectlyInSelectList && this.scalarFunctionsCast.contains(function.getFunction())) {
            // Cast to FLOAT because result set metadata has precision = 0, scale = 0
            sql = cast(sql, "FLOAT");
        }
        return sql;
    }

    private String getTrim(final SqlFunctionScalar function) throws AdapterException {
        final List<SqlNode> arguments = function.getArguments();
        final List<String> argumentsSql = new ArrayList<>(arguments.size());
        for (final SqlNode node : arguments) {
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

    private String getLocate(final SqlFunctionScalar function) throws AdapterException {
        final List<SqlNode> arguments = function.getArguments();
        final List<String> argumentsSql = new ArrayList<>(arguments.size());
        for (final SqlNode node : arguments) {
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
        return builder.toString();
    }

    private String getTimeOrDate(final SqlFunctionScalar function) throws AdapterException {
        final List<SqlNode> arguments = function.getArguments();
        final List<String> argumentsSql = new ArrayList<>(arguments.size());
        for (final SqlNode node : arguments) {
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
        return builder.toString();
    }

    private String getSqlFunctionScalar(final SqlFunctionScalar function, final String s, final String s2)
            throws AdapterException {
        final List<SqlNode> arguments = function.getArguments();
        final List<String> argumentsSql = new ArrayList<>(arguments.size());
        for (final SqlNode node : arguments) {
            argumentsSql.add(node.accept(this));
        }
        final StringBuilder builder = new StringBuilder();
        builder.append(s);
        builder.append(argumentsSql.get(0));
        builder.append(s2);
        return builder.toString();
    }

    private String getRepeat(final SqlFunctionScalar function) throws AdapterException {
        final List<SqlNode> arguments = function.getArguments();
        final List<String> argumentsSql = new ArrayList<>(arguments.size());
        for (final SqlNode node : arguments) {
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
        return builder.toString();
    }

    private String getDiv(final SqlFunctionScalar function) throws AdapterException {
        final List<SqlNode> arguments = function.getArguments();
        final List<String> argumentsSql = new ArrayList<>(arguments.size());
        for (final SqlNode node : arguments) {
            argumentsSql.add(node.accept(this));
        }
        final StringBuilder builder = new StringBuilder();
        builder.append("CAST(FLOOR(");
        builder.append(argumentsSql.get(0));
        builder.append(" / ");
        builder.append(argumentsSql.get(1));
        builder.append(") AS NUMBER(36, 0))");
        return builder.toString();
    }
}
