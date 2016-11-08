package com.exasol.adapter.sql;

public abstract class SqlPredicate extends SqlNode {

    private Predicate function;

    public SqlPredicate(Predicate function) {
        this.function = function;
    }

    public Predicate getFunction() {
        return function;
    }
}
