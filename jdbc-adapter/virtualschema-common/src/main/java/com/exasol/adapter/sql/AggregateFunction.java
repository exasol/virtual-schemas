package com.exasol.adapter.sql;

/**
 * List of all aggregation functions supported by EXASOL
 */
public enum AggregateFunction {
    COUNT,
    SUM,
    MIN,
    MAX,
    AVG,

    MEDIAN,

    FIRST_VALUE,
    LAST_VALUE,

    STDDEV,
    STDDEV_POP,
    STDDEV_SAMP,

    VARIANCE,
    VAR_POP,
    VAR_SAMP,

    GROUP_CONCAT(false),

    APPROXIMATE_COUNT_DISTINCT,

    GEO_INTERSECTION_AGGREGATE,
    GEO_UNION_AGGREGATE;

    private boolean isSimple;

    /**
     * True if the function is simple, i.e. is handled by {@link SqlFunctionAggregate}, and false if it has it's own implementation.
     */
    public boolean isSimple() {
        return isSimple;
    }

    AggregateFunction() {
        this.isSimple = true;
    }

    AggregateFunction(boolean isSimple) {
        this.isSimple = isSimple;
    }

}
