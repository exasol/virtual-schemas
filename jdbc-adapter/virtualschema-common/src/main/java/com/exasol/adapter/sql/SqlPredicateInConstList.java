package com.exasol.adapter.sql;

import com.exasol.adapter.AdapterException;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class SqlPredicateInConstList extends SqlPredicate {
    
    // For <exp> IN (...) this stores <exp>
    SqlNode expression;
    // Arguments inside the brackets
    List<SqlNode> inArguments;
    
    public SqlPredicateInConstList(SqlNode expression, List<SqlNode> inArguments) {
        super(Predicate.IN_CONSTLIST);
        this.expression = expression;
        this.inArguments = inArguments;
        if (this.expression != null) {
            this.expression.setParent(this);
        }
        if (this.inArguments != null) {
            for (SqlNode node : this.inArguments) {
                node.setParent(this);
            }
        }
    }
    
    public SqlNode getExpression() {
        return expression;
    }
    
    public List<SqlNode> getInArguments() {
        if (inArguments == null) {
            return null;
        } else {
            return Collections.unmodifiableList(inArguments);
        }
    }
    
    @Override
    public String toSimpleSql() {
        List<String> argumentsSql = new ArrayList<>();
        for (SqlNode node : inArguments) {
            argumentsSql.add(node.toSimpleSql());
        }
        return expression.toSimpleSql() + " IN (" + Joiner.on(", ").join(argumentsSql) + ")";
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.PREDICATE_IN_CONSTLIST;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) throws AdapterException {
        return visitor.visit(this);
    }

}
