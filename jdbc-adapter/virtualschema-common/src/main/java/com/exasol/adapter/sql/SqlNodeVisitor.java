package com.exasol.adapter.sql;

import com.exasol.adapter.AdapterException;

/**
 * Implementation of the Visitor pattern for the SqlNode.
 * 
 * Benefit of this Visitor implementation: We get compile time safety that all
 * Visitors have implementations for all SqlNode types.
 * 
 * Drawback of this Visitor implementation: Whenever a new SqlNode gets added,
 * we need to implement it here (should be fine for now). If this becomes to
 * annoying, we can still switch to a visitor pattern using Reflection.
 */
public interface SqlNodeVisitor<R> {

    public R visit(SqlStatementSelect select) throws AdapterException;

    public R visit(SqlSelectList selectList) throws AdapterException;

    public R visit(SqlGroupBy groupBy) throws AdapterException;

    public R visit(SqlColumn sqlColumn) throws AdapterException;

    public R visit(SqlFunctionAggregate sqlFunctionAggregate) throws AdapterException;

    public R visit(SqlFunctionAggregateGroupConcat sqlFunctionAggregateGroupConcat) throws AdapterException;

    public R visit(SqlFunctionScalar sqlFunctionScalar) throws AdapterException;

    public R visit(SqlFunctionScalarCase sqlFunctionScalarCase) throws AdapterException;

    public R visit(SqlFunctionScalarCast sqlFunctionScalarCast) throws AdapterException;

    public R visit(SqlFunctionScalarExtract sqlFunctionScalarExtract) throws AdapterException;

    public R visit(SqlLimit sqlLimit) throws AdapterException;

    public R visit(SqlLiteralBool sqlLiteralBool) throws AdapterException;

    public R visit(SqlLiteralDate sqlLiteralDate) throws AdapterException;

    public R visit(SqlLiteralDouble sqlLiteralDouble) throws AdapterException;

    public R visit(SqlLiteralExactnumeric sqlLiteralExactnumeric) throws AdapterException;

    public R visit(SqlLiteralNull sqlLiteralNull) throws AdapterException;

    public R visit(SqlLiteralString sqlLiteralString) throws AdapterException;

    public R visit(SqlLiteralTimestamp sqlLiteralTimestamp) throws AdapterException;

    public R visit(SqlLiteralTimestampUtc sqlLiteralTimestampUtc) throws AdapterException;

    public R visit(SqlLiteralInterval sqlLiteralInterval) throws AdapterException;

    public R visit(SqlOrderBy sqlOrderBy) throws AdapterException;

    public R visit(SqlPredicateAnd sqlPredicateAnd) throws AdapterException;

    public R visit(SqlPredicateBetween sqlPredicateBetween) throws AdapterException;

    public R visit(SqlPredicateEqual sqlPredicateEqual) throws AdapterException;

    public R visit(SqlPredicateInConstList sqlPredicateInConstList) throws AdapterException;

    public R visit(SqlPredicateLess sqlPredicateLess) throws AdapterException;

    public R visit(SqlPredicateLessEqual sqlPredicateLessEqual) throws AdapterException;

    public R visit(SqlPredicateLike sqlPredicateLike) throws AdapterException;

    public R visit(SqlPredicateLikeRegexp sqlPredicateLikeRegexp) throws AdapterException;

    public R visit(SqlPredicateNot sqlPredicateNot) throws AdapterException;

    public R visit(SqlPredicateNotEqual sqlPredicateNotEqual) throws AdapterException;

    public R visit(SqlPredicateOr sqlPredicateOr) throws AdapterException;

    public R visit(SqlPredicateIsNotNull sqlPredicateOr) throws AdapterException;

    public R visit(SqlPredicateIsNull sqlPredicateOr) throws AdapterException;

    public R visit(SqlTable sqlTable) throws AdapterException;

}
