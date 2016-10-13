package com.exasol.adapter.capabilities;

import com.exasol.adapter.sql.Predicate;

/**
 * List of all Predicates (scalar functions returning bool) supported by EXASOL.
 */
public enum PredicateCapability {
    AND,
    OR,
    NOT,
    EQUAL,
    NOTEQUAL,
    LESS,
    LESSEQUAL,
    LIKE,
    LIKE_ESCAPE (Predicate.LIKE),    // the LIKE predicate with the optional escape character defined
    REGEXP_LIKE,
    BETWEEN,
    IN_CONSTLIST,
    IS_NULL,
    IS_NOT_NULL;

    private Predicate predicate;

    PredicateCapability() {
        this.predicate = Predicate.valueOf(name());
    }

    PredicateCapability(Predicate predicate) {
        this.predicate = predicate;
    }

    public Predicate getPredicate() {
        return predicate;
    }
}

