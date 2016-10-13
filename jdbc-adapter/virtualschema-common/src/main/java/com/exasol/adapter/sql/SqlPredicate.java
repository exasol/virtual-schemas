package com.exasol.adapter.sql;

import java.util.List;

public abstract class SqlPredicate extends SqlNode {

    private Predicate function;

    public SqlPredicate(Predicate function) {
        this.function = function;
    }

    public SqlPredicate(List<SqlNode> sons, Predicate function) {
        super(sons);
        this.function = function;
    }

    public Predicate getFunction() {
        return function;
    }
}
