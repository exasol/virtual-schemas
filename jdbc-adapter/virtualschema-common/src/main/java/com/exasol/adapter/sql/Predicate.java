package com.exasol.adapter.sql;

/**
 * List of all predicates (scalar functions returning bool) supported by EXASOL.
 */
public enum Predicate {
    AND,
    OR,
    NOT,
    EQUAL,
    NOTEQUAL,
    LESS,
    LESSEQUAL,
    LIKE,
    REGEXP_LIKE,
    BETWEEN,
    IN_CONSTLIST,
    IS_NULL,
    IS_NOT_NULL
}
