package com.exasol.adapter.json;

import com.exasol.adapter.metadata.*;
import com.exasol.adapter.metadata.DataType.ExaCharset;
import com.exasol.adapter.metadata.DataType.IntervalType;
import com.exasol.adapter.request.*;
import com.exasol.adapter.sql.*;
import com.exasol.utils.JsonHelper;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RequestJsonParser {
    
    private List<TableMetadata> involvedTablesMetadata;
    
    public AdapterRequest parseRequest(String json) throws Exception {
        JsonObject root = JsonHelper.getJsonObject(json);
        String requestType = root.getString("type","");
        SchemaMetadataInfo meta = parseMetadataInfo(root);
        if (requestType.equals("createVirtualSchema")) {
            return new CreateVirtualSchemaRequest(meta);
        } else if (requestType.equals("dropVirtualSchema")) {
            return new DropVirtualSchemaRequest(meta);
        } else if (requestType.equals("refresh")) {
            if (root.containsKey("requestedTables")) {
                List<String> tables = new ArrayList<>();
                for (JsonString table : root.getJsonArray("requestedTables").getValuesAs(JsonString.class)) {
                    tables.add(table.getString());
                }
                return new RefreshRequest(meta, tables);
            } else {
                return new RefreshRequest(meta);
            }
        } else if (requestType.equals("setProperties")) {
            Map<String, String> properties = new HashMap<String, String>();
            assert(root.containsKey("properties") && root.get("properties").getValueType() == ValueType.OBJECT);
            for (Map.Entry<String, JsonValue> entry : root.getJsonObject("properties").entrySet()) {
                String key = entry.getKey();
                // Null values represent properties which are deleted by the user (might also have never existed actually)
                if (root.getJsonObject("properties").isNull(key)) {
                    properties.put(key.toUpperCase(), null);
                } else {
                    properties.put(key.toUpperCase(), root.getJsonObject("properties").getString(key));
                }
            }
            return new SetPropertiesRequest(meta, properties);
        } else if (requestType.equals("getCapabilities")) {
            return new GetCapabilitiesRequest(meta);
        } else if (requestType.equals("pushdown")) {
            assert(root.containsKey("involvedTables") && root.get("involvedTables").getValueType() == ValueType.ARRAY);
            involvedTablesMetadata = parseInvolvedTableMetadata(root.getJsonArray("involvedTables"));
            JsonObject pushdownExp;
            if (root.containsKey("pushdownRequest")) {
                pushdownExp = root.getJsonObject("pushdownRequest");
            } else {
                pushdownExp = root.getJsonObject("pushdownInquiry");    // This is outdated, remove when old versions are no longer used
            }
            SqlNode select = parseExpression(pushdownExp);
            assert(select.getType() == SqlNodeType.SELECT);
            return new PushdownRequest(meta, (SqlStatementSelect)select, involvedTablesMetadata);
        } else {
            throw new RuntimeException("Request Type not supported: " + requestType);
        }
    }
    
    private List<TableMetadata> parseInvolvedTableMetadata(JsonArray involvedTables) throws MetadataException {
        List<TableMetadata> tables = new ArrayList<>();
        for (JsonObject table : involvedTables.getValuesAs(JsonObject.class)) {
            String tableName = table.getString("name","");
            String tableAdapterNotes = readAdapterNotes(table);
            String tableComment = table.getString("comment", "");
            List<ColumnMetadata> columns = new ArrayList<>();
            for (JsonObject column : table.getJsonArray("columns").getValuesAs(JsonObject.class)) {
                columns.add(parseColumnMetadata(column));
            }
            tables.add(new TableMetadata(tableName, tableAdapterNotes, columns, tableComment));
        }
        return tables;
    }
    
    private ColumnMetadata parseColumnMetadata(JsonObject column) throws MetadataException {
        String columnName = column.getString("name");
        String adapterNotes = readAdapterNotes(column);
        String comment = column.getString("comment", "");
        String defaultValue = column.getString("default", "");
        boolean isNullable = true;
        if (column.containsKey("isNullable")) {
            isNullable = column.getBoolean("isNullable");
        }
        boolean isIdentity = true;
        if (column.containsKey("isIdentity")) {
            isIdentity = column.getBoolean("isIdentity");
        }
        JsonObject dataType = column.getJsonObject("dataType");
        DataType type = getDataType(dataType);
        return new ColumnMetadata(columnName, adapterNotes, type, isNullable, isIdentity, defaultValue, comment);
    }

    private DataType getDataType(JsonObject dataType) throws MetadataException {
        String typeName = dataType.getString("type").toUpperCase();
        DataType type = null;
        if (typeName.equals("DECIMAL")) {
            type = DataType.createDecimal(dataType.getInt("precision"), dataType.getInt("scale"));
        } else if (typeName.equals("DOUBLE")) {
            type = DataType.createDouble();
        } else if (typeName.equals("VARCHAR")) {
            String charSet = dataType.getString("characterSet", "UTF8");
            type = DataType.createVarChar(dataType.getInt("size"), charSetFromString(charSet));
        } else if (typeName.equals("CHAR")) {
            String charSet = dataType.getString("characterSet", "UTF8");
            type = DataType.createChar(dataType.getInt("size"), charSetFromString(charSet));
        } else if (typeName.equals("BOOLEAN")) {
            type = DataType.createBool();
        } else if (typeName.equals("DATE")) {
            type = DataType.createDate();
        } else if (typeName.equals("TIMESTAMP")) {
            boolean withLocalTimezone = dataType.getBoolean("withLocalTimeZone", false);
            type = DataType.createTimestamp(withLocalTimezone);
        } else if (typeName.equals("INTERVAL")) {
            int precision = dataType.getInt("precision", 2);    // has a default in EXASOL
            IntervalType intervalType = intervalTypeFromString(dataType.getString("fromTo"));
            if (intervalType == IntervalType.DAY_TO_SECOND) {
                int fraction = dataType.getInt("fraction", 3);      // has a default in EXASOL
                type = DataType.createIntervalDaySecond(precision, fraction);
            } else {
                assert(intervalType == IntervalType.YEAR_TO_MONTH);
                type = DataType.createIntervalYearMonth(precision);
            }
        } else if (typeName.equals("GEOMETRY")) {
            int srid = dataType.getInt("srid");
            type = DataType.createGeometry(srid);
        } else {
            throw new MetadataException("Unsupported data type encountered: " + typeName);
        }
        return type;
    }

    private static IntervalType intervalTypeFromString(String intervalType) throws MetadataException {
        if (intervalType.equals("DAY TO SECONDS")) {
            return IntervalType.DAY_TO_SECOND;
        } else if (intervalType.equals("YEAR TO MONTH")) {
            return IntervalType.YEAR_TO_MONTH;
        } else {
            throw new MetadataException("Unsupported interval data type encountered: " + intervalType);
        }
    }
    
    private static ExaCharset charSetFromString(String charset) throws MetadataException {
        if (charset.equals("UTF8")) {
            return ExaCharset.UTF8;
        } else if (charset.equals("ASCII")) {
            return ExaCharset.ASCII;
        } else {
            throw new MetadataException("Unsupported charset encountered: " + charset);
        }
    }

    private SqlStatementSelect parseSelect(JsonObject select) throws MetadataException {
        // FROM clause
        SqlNode table = parseExpression(select.getJsonObject("from"));
        assert(table.getType() == SqlNodeType.TABLE || table.getType() == SqlNodeType.JOIN);
        // SELECT list
        SqlSelectList selectList = parseSelectList(select.getJsonArray("selectList"));
        // GROUP BY
//        boolean hasAggregation = false;
//        boolean hasGroupBy = false;
//        if (select.containsKey("aggregationType")) {
//            hasAggregation = true;
//            hasGroupBy = select.getString("aggregationType").equals("group_by");
//        }
        SqlExpressionList groupByClause = parseGroupBy(select.getJsonArray("groupBy"));
        
        // WHERE clause
        SqlNode whereClause = null;
        if (select.containsKey("filter")) {
            whereClause = parseExpression(select.getJsonObject("filter"));
        }
        SqlNode having = null;
        if (select.containsKey("having")) {
            having = parseExpression(select.getJsonObject("having"));
        }
        SqlOrderBy orderBy = null;
        if (select.containsKey("orderBy")) {
            orderBy = parseOrderBy(select.getJsonArray("orderBy"));
        }
        SqlLimit limit = null;
        if (select.containsKey("limit")) {
            limit = parseLimit(select.getJsonObject("limit"));
        }
        return new SqlStatementSelect(table, selectList, whereClause, groupByClause, having, orderBy, limit);
    }
    
    private List<SqlNode> parseExpressionList(JsonArray array) throws MetadataException {
        assert(array != null);
        List<SqlNode> sqlNodes = new ArrayList<>();
        for (JsonObject expr : array.getValuesAs(JsonObject.class)) {
            SqlNode node = parseExpression(expr);
            sqlNodes.add(node);
        }
        return sqlNodes;
    }

    private SqlGroupBy parseGroupBy(JsonArray groupBy) throws MetadataException {
        if (groupBy == null) {
            return null;
        }
        List<SqlNode> groupByElements = parseExpressionList(groupBy);
        return new SqlGroupBy(groupByElements);
    }

    private SqlSelectList parseSelectList(JsonArray selectList) throws MetadataException {
        if (selectList == null) {
            // this is like SELECT *
            return SqlSelectList.createSelectStarSelectList();
        }
        List<SqlNode> selectListElements = parseExpressionList(selectList);
        if (selectListElements.size() == 0) {
            return SqlSelectList.createAnyValueSelectList();
        } else {
            return SqlSelectList.createRegularSelectList(selectListElements);
        }
    }
    
    private SqlOrderBy parseOrderBy(JsonArray orderByList) throws MetadataException {
        List<SqlNode> orderByExpressions = new ArrayList<>();
        List<Boolean> isAsc = new ArrayList<>();
        List<Boolean> nullsLast = new ArrayList<>();
        for (int i=0; i<orderByList.size(); ++i) {
            JsonObject orderElem = orderByList.getJsonObject(i);
            orderByExpressions.add(parseExpression(orderElem.getJsonObject("expression")));
            isAsc.add(orderElem.getBoolean("isAscending", true));
            nullsLast.add(orderElem.getBoolean("nullsLast", true));
        }
        return new SqlOrderBy(orderByExpressions, isAsc, nullsLast);
    }
    
    private SqlLimit parseLimit(JsonObject limit) {
        int numElements = limit.getInt("numElements");
        int offset = limit.getInt("offset", 0);
        return new SqlLimit(numElements, offset);
    }

    private SchemaMetadataInfo parseMetadataInfo(JsonObject root) {
        JsonObject meta = root.getJsonObject("schemaMetadataInfo");
        if (meta == null) {
            return null;
        }
        String schemaName = meta.getString("name");
        String schemaAdapterNotes = readAdapterNotes(meta);
        Map<String, String> properties = new HashMap<String, String>();
        if (meta.getJsonObject("properties") != null) {
            for (Map.Entry<String, JsonValue> entry : meta.getJsonObject("properties").entrySet()) {
                String key = entry.getKey();
                properties.put(key.toUpperCase(), meta.getJsonObject("properties").getString(key));
            }
        }
        return new SchemaMetadataInfo(schemaName, schemaAdapterNotes, properties);
    }
    
    private static String readAdapterNotes(JsonObject root) {
        if (root.containsKey("adapterNotes")) {
            JsonValue notes = root.get("adapterNotes");
            if (notes.getValueType() == ValueType.STRING) {
                // Return unquoted string
                return ((JsonString)notes).getString();
            } else {
                return notes.toString();
            }
        }
        return "";
    }

    private SqlNode parseExpression(JsonObject exp) throws MetadataException {
        String typeName = exp.getString("type", "");
        SqlNodeType type = fromTypeName(typeName);
        switch (type) {
        case SELECT:
            return parseSelect(exp);
        case TABLE: {
            String tableName = exp.getString("name");
            TableMetadata tableMetadata = findInvolvedTableMetadata(tableName);
            if (exp.containsKey("alias")) {
                String tableAlias = exp.getString("alias");
                return new SqlTable(tableName, tableAlias, tableMetadata);
            } else {
                return new SqlTable(tableName, tableMetadata);
            }
        }
        case JOIN: {
            SqlNode left = parseExpression(exp.getJsonObject("left"));
            SqlNode right = parseExpression(exp.getJsonObject("right"));
            SqlNode condition = parseExpression(exp.getJsonObject("condition"));
            JoinType joinType = fromJoinTypeName(exp.getString("join_type"));
            return new SqlJoin(left, right, condition, joinType);
        }
        case COLUMN: {
            int columnId = exp.getInt("columnNr");
            String columnName = exp.getString("name");
            String tableName = exp.getString("tableName");
            ColumnMetadata columnMetadata = findColumnMetadata(tableName, columnName);
            if (exp.containsKey("tableAlias")) {
                String tableAlias = exp.getString("tableAlias");
                return new SqlColumn(columnId, columnMetadata, tableName, tableAlias);
            } else {
                return new SqlColumn(columnId, columnMetadata, tableName);
            }
        }
        case LITERAL_NULL: {
            return new SqlLiteralNull();
        }
        case LITERAL_BOOL: {
            boolean boolVal = exp.getBoolean("value");
            return new SqlLiteralBool(boolVal);
        }
        case LITERAL_DATE: {
            String date = exp.getString("value");
            return new SqlLiteralDate(date);
        }
        case LITERAL_TIMESTAMP: {
            String timestamp = exp.getString("value");
            return new SqlLiteralTimestamp(timestamp);
        }
        case LITERAL_TIMESTAMPUTC: {
            String timestampUtc = exp.getString("value");
            return new SqlLiteralTimestampUtc(timestampUtc);
        }
        case LITERAL_DOUBLE: {
            String doubleString = exp.getString("value");
            return new SqlLiteralDouble(Double.parseDouble(doubleString));
        }
        case LITERAL_EXACTNUMERIC: {
            BigDecimal exactVal = new BigDecimal( exp.getString("value"));
            return new SqlLiteralExactnumeric(exactVal);
        }
        case LITERAL_STRING: {
            String stringVal = exp.getString("value");
            return new SqlLiteralString(stringVal);
        }
        case LITERAL_INTERVAL: {
            String intervalVal = exp.getString("value");
            DataType intervalType = getDataType(exp.getJsonObject("dataType"));
            return new SqlLiteralInterval(intervalVal, intervalType);
        }
        case PREDICATE_AND: {
            List<SqlNode> andedPredicates = new ArrayList<>();
            for (JsonObject pred : exp.getJsonArray("expressions").getValuesAs(JsonObject.class)) {
                andedPredicates.add(parseExpression(pred));
            }
            return new SqlPredicateAnd(andedPredicates);
        }
        case PREDICATE_OR: {
            List<SqlNode> orPredicates = new ArrayList<>();
            for (JsonObject pred : exp.getJsonArray("expressions").getValuesAs(JsonObject.class)) {
                orPredicates.add(parseExpression(pred));
            }
            return new SqlPredicateOr(orPredicates);
        }
        case PREDICATE_NOT: {
            SqlNode notExp = parseExpression(exp.getJsonObject("expression"));
            return new SqlPredicateNot(notExp);
        }
        case PREDICATE_EQUAL: {
            SqlNode equalLeft = parseExpression(exp.getJsonObject("left"));
            SqlNode equalRight = parseExpression(exp.getJsonObject("right"));
            return new SqlPredicateEqual(equalLeft, equalRight);
        }
        case PREDICATE_NOTEQUAL: {
            SqlNode notEqualLeft = parseExpression(exp.getJsonObject("left"));
            SqlNode notEqualRight = parseExpression(exp.getJsonObject("right"));
            return new SqlPredicateNotEqual(notEqualLeft, notEqualRight);
        }
        case PREDICATE_LESS: {
            SqlNode lessLeft = parseExpression(exp.getJsonObject("left"));
            SqlNode lessRight = parseExpression(exp.getJsonObject("right"));
            return new SqlPredicateLess(lessLeft, lessRight);
        }
        case PREDICATE_LESSEQUAL: {
            SqlNode lessEqLeft = parseExpression(exp.getJsonObject("left"));
            SqlNode lessEqRight = parseExpression(exp.getJsonObject("right"));
            return new SqlPredicateLessEqual(lessEqLeft, lessEqRight);
        }
        case PREDICATE_LIKE: {
            SqlNode likeLeft = parseExpression(exp.getJsonObject("expression"));
            SqlNode likePattern = parseExpression(exp.getJsonObject("pattern"));
            if (exp.containsKey("escapeChar")) {
                SqlNode escapeChar = parseExpression(exp.getJsonObject("escapeChar"));
                return new SqlPredicateLike(likeLeft, likePattern, escapeChar);
            }
            return new SqlPredicateLike(likeLeft, likePattern);
        }
        case PREDICATE_LIKE_REGEXP: {
            SqlNode likeRegexpLeft = parseExpression(exp.getJsonObject("expression"));
            SqlNode likeRegexpPattern = parseExpression(exp.getJsonObject("pattern"));
            return new SqlPredicateLikeRegexp(likeRegexpLeft, likeRegexpPattern);
        }
        case PREDICATE_BETWEEN: {
            SqlNode betweenExp = parseExpression(exp.getJsonObject("expression"));
            SqlNode betweenLeft = parseExpression(exp.getJsonObject("left"));
            SqlNode betweenRight = parseExpression(exp.getJsonObject("right"));
            return new SqlPredicateBetween(betweenExp, betweenLeft, betweenRight);
        }
        case PREDICATE_IN_CONSTLIST: {
            SqlNode inExp = parseExpression(exp.getJsonObject("expression"));
            List<SqlNode> inArguments = new ArrayList<>();
            for (JsonObject pred : exp.getJsonArray("arguments").getValuesAs(JsonObject.class)) {
                inArguments.add(parseExpression(pred));
            }
            return new SqlPredicateInConstList(inExp, inArguments);
        }
        case PREDICATE_IS_NULL: {
            SqlNode isnullExp = parseExpression(exp.getJsonObject("expression"));
            return new SqlPredicateIsNull(isnullExp);
        }
        case PREDICATE_IS_NOT_NULL: {
            SqlNode isNotnullExp = parseExpression(exp.getJsonObject("expression"));
            return new SqlPredicateIsNotNull(isNotnullExp);
        }
        case FUNCTION_SCALAR: {
            String functionName = exp.getString("name");
            boolean hasVariableInputArgs = false;
            int numArgs;
            if (exp.containsKey("variableInputArgs")) {
                hasVariableInputArgs = exp.getBoolean("variableInputArgs");
            }
            List<SqlNode> arguments = new ArrayList<>();
            for (JsonObject argument : exp.getJsonArray("arguments").getValuesAs(JsonObject.class)) {
                arguments.add(parseExpression(argument));
            }
            if (!hasVariableInputArgs) {
                numArgs = exp.getInt("numArgs");    // this is the expected number of arguments for this scalar function
                assert (numArgs == arguments.size());
            }
            boolean isInfix = false;
            if (exp.containsKey("infix")) {
                isInfix = exp.getBoolean("infix");
            }
            boolean isPrefix = false;
            if (exp.containsKey("prefix")) {
                assert (!isPrefix);
                isPrefix = exp.getBoolean("prefix");
            }
            return new SqlFunctionScalar(fromScalarFunctionName(functionName), arguments, isInfix, isPrefix);
        }
        case FUNCTION_SCALAR_EXTRACT: {
            String toExtract = exp.getString("toExtract");
            List<SqlNode> extractArguments = new ArrayList<>();
            if (exp.containsKey("arguments")) {
                for (JsonObject argument : exp.getJsonArray("arguments").getValuesAs(JsonObject.class)) {
                    extractArguments.add(parseExpression(argument));
                }
            }
            return new SqlFunctionScalarExtract(toExtract, extractArguments);
        }
        case FUNCTION_SCALAR_CASE: {
            List<SqlNode> caseArguments = new ArrayList<>();
            List<SqlNode> caseResults = new ArrayList<>();
            SqlNode caseBasis = null;
            if (exp.containsKey("arguments")) {
                for (JsonObject argument : exp.getJsonArray("arguments").getValuesAs(JsonObject.class)) {
                    caseArguments.add(parseExpression(argument));
                }
            }
            if (exp.containsKey("results")) {
                for (JsonObject argument : exp.getJsonArray("results").getValuesAs(JsonObject.class)) {
                    caseResults.add(parseExpression(argument));
                }
            }
            if (exp.containsKey("basis")) {
                caseBasis = parseExpression(exp.getJsonObject("basis"));
            }
            return new SqlFunctionScalarCase(caseArguments, caseResults, caseBasis);
        }
        case FUNCTION_SCALAR_CAST: {
            DataType castDataType = getDataType(exp.getJsonObject("dataType"));
            List<SqlNode> castArguments = new ArrayList<>();
            if (exp.containsKey("arguments")) {
                for (JsonObject argument : exp.getJsonArray("arguments").getValuesAs(JsonObject.class)) {
                    castArguments.add(parseExpression(argument));
                }
            }
            return new SqlFunctionScalarCast(castDataType, castArguments);
        }
        case FUNCTION_AGGREGATE: {
            String setFunctionName = exp.getString("name");
            List<SqlNode> setArguments = new ArrayList<>();
            boolean distinct = false;
            if (exp.containsKey("distinct")) {
                distinct = exp.getBoolean("distinct");
            }
            if (exp.containsKey("arguments")) {
                for (JsonObject argument : exp.getJsonArray("arguments").getValuesAs(JsonObject.class)) {
                    setArguments.add(parseExpression(argument));
                }
            }
            return new SqlFunctionAggregate(fromAggregationFunctionName(setFunctionName), setArguments, distinct);
        }
        case FUNCTION_AGGREGATE_GROUP_CONCAT: {
            String functionName = exp.getString("name");
            List<SqlNode> setArguments = new ArrayList<>();
            boolean distinct = false;
            if (exp.containsKey("distinct")) {
                distinct = exp.getBoolean("distinct");
            }
            if (exp.containsKey("arguments")) {
                for (JsonObject argument : exp.getJsonArray("arguments").getValuesAs(JsonObject.class)) {
                    setArguments.add(parseExpression(argument));
                }
            }
            SqlOrderBy orderBy = null;
            if (exp.containsKey("orderBy")) {
                orderBy = parseOrderBy(exp.getJsonArray("orderBy"));
            }
            String separator = null;
            if (exp.containsKey("separator")) {
                separator = exp.getString("separator");
            }
            return new SqlFunctionAggregateGroupConcat(fromAggregationFunctionName(functionName),
                    setArguments, orderBy, distinct, separator);
        }
        default:
            throw new RuntimeException("Unknown node type: " + typeName);
        }
    }

    /**
     * Mapping from join type name (as in json api) to enum
     */
    private static JoinType fromJoinTypeName(String typeName) {
        return Enum.valueOf(JoinType.class, typeName.toUpperCase());
    }

    /**
     * Mapping from scalar function name (as in json api) to enum
     */
    private static ScalarFunction fromScalarFunctionName(String functionName) {
        return Enum.valueOf(ScalarFunction.class, functionName.toUpperCase());
    }

    /**
     * Mapping from aggregate function name (as in json api) to enum
     */
    private static AggregateFunction fromAggregationFunctionName(String functionName) {
        return Enum.valueOf(AggregateFunction.class, functionName.toUpperCase());
    }

    /**
     * Mapping from type name (as in json api) to enum
     */
    private static SqlNodeType fromTypeName(String typeName) {
        return Enum.valueOf(SqlNodeType.class, typeName.toUpperCase());
    }

    private TableMetadata findInvolvedTableMetadata(String tableName) throws MetadataException {
        assert(involvedTablesMetadata != null);
        for (TableMetadata tableMetadata : involvedTablesMetadata) {
            if (tableMetadata.getName().equals(tableName)) {
                return tableMetadata;
            }
        }
        throw new MetadataException("Could not find table metadata for involved table " + tableName + ". All involved tables: " + involvedTablesMetadata.toString());
    }

    private ColumnMetadata findColumnMetadata(String tableName, String columnName) throws MetadataException {
        TableMetadata tableMetadata = findInvolvedTableMetadata(tableName);
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            if (columnMetadata.getName().equals(columnName)) {
                return columnMetadata;
            }
        }
        throw new MetadataException("Could not find column metadata for involved table " + tableName + " and column + " + columnName + ". All involved tables: " + involvedTablesMetadata.toString());
    }
}
