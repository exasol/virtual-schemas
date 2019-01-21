package com.exasol.adapter.capabilities;

/**
 * This is an enumeration of the main capabilities supported by the EXASOL Virtual Schema Framework.
 */
public enum MainCapability {

    /**
     * Support projections, i.e. request only a subset of all columns of the
     * table.
     *
     * <p>Example: SELECT a FROM t;</p>
     */
    SELECTLIST_PROJECTION,

    /**
     * Support expressions in the select list;
     * Additional capabilities are required depending on the expression.
     *
     * <p>Example: SELECT a+1, ucase(b) FROM t;</p>
     */
    SELECTLIST_EXPRESSIONS,

    /**
     * Support filter expressions. Additional capabilities are required
     * depending on the expression.
     *
     * <p>Example: SELECT * FROM t WHERE a>2</p>
     */
    FILTER_EXPRESSIONS,

    /**
     * Support aggregations with a single group. This happens when an
     * aggregation-function is used without a group by clause.
     *
     * <p>Example: SELECT min(a) FROM t;</p>
     */
    AGGREGATE_SINGLE_GROUP,

    /**
     * Support aggregations with a group by clause consisting of columns.
     *
     * <p>Example: SELECT a, b, min(c) FROM t GROUP BY a, b;</p>
     */
    AGGREGATE_GROUP_BY_COLUMN,

    /**
     * Support aggregations with a group by clause that contains expressions.
     *
     * <p>Example: SELECT a+1, min(b) FROM t GROUP BY a+1;</p>
     */
    AGGREGATE_GROUP_BY_EXPRESSION,

    /**
     * Support aggregations with a group by clause with multiple group by
     * columns or expressions.
     * Note that you might additionally require AGGREGATE_GROUP_BY_EXPRESSION or
     * AGGREGATE_GROUP_BY_COLUMN.
     *
     * <p>Example: SELECT a, b, min(c) FROM t GROUP BY a, b;</p>
     */
    AGGREGATE_GROUP_BY_TUPLE,

    /**
     * Support aggregations with a having clause. Additional capabilities might
     * be required depending on the expression.
     *
     * <p>Example: SELECT a, min(b) FROM t GROUP BY a HAVING min(b) > 10;</p>
     */
    AGGREGATE_HAVING,

    /**
     * Support to order the result by columns.
     *
     * <p>Attention: This includes the capability to specify NULLS FIRST/LAST and ASC/DESC.</p>
     * 
     * <p>Example: SELECT a, b FROM t ORDER BY a, b DESC NULLS FIRST;</p>
     */
    ORDER_BY_COLUMN,

    /**
     * Support to order the result by expressions.
     *
     * <p>Attention: This includes the capability to specify NULLS FIRST/LAST and ASC/DESC</p>
     * 
     * <p>Example: SELECT a FROM t ORDER BY abs(a) ASC NULLS LAST;</p>
     */
    ORDER_BY_EXPRESSION,

    /**
     * Support to limit the number of result rows. Often used together with ordering.
     *
     * <p>Example: SELECT * FROM t 0</p>
     */
    LIMIT,

    /**
     * Support to limit the number of result rows including an offset.
     *
     * <p>Example: SELECT * FROM t LIMIT 100 OFFSET 10;</p>
     */
    LIMIT_WITH_OFFSET//,

    /**
     * Support inner joins.
     *
     * <p>Example: SELECT * FROM t INNER JOIN u ON t.id = u.id;</p>
     */
    //JOIN_TYPE_INNER,

    /**
     * Support left outer joins.
     *
     * <p>Example: SELECT * FROM t LEFT OUTER JOIN u ON t.id = u.id;</p>
     */
    //JOIN_TYPE_LEFT_OUTER,

     /**
     * Support right outer joins.
     *
     * <p>Example: SELECT * FROM t RIGHT OUTER JOIN u ON t.id = u.id;</p>
     */
    //JOIN_TYPE_RIGHT_OUTER,

     /**
     * Support full outer joins.
     *
     * <p>Example: SELECT * FROM t FULL OUTER JOIN u ON t.id = u.id;</p>
     */
    //JOIN_TYPE_FULL_OUTER,

    /**
     * Support joins with equi conditions.
     *
     * <p>Example: SELECT * FROM t INNER JOIN u ON t.id = u.id;</p>
     * <p>SELECT * FROM t INNER JOIN u ON t.id = u.id + 3;</p>
     * <p>SELECT * FROM t INNER JOIN u ON ((substr(sqrt(t.x), 2) || t.y = 'a') AND t.z > 0)   =   (u.b OR u.y + 3 > 0) </p>
     */
    //JOIN_CONDITION_EQUI,

    /**
     * Support joins with any conditions.
     *
     * <p>Example: SELECT * FROM t INNER JOIN u ON t.x = between(u.x, u.y) ;</p>
     */
    //JOIN_CONDITION_ALL
}
