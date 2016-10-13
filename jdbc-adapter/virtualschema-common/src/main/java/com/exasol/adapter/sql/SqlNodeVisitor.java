package com.exasol.adapter.sql;

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

    public R visit(SqlStatementSelect select);

    public R visit(SqlSelectList selectList);

    public R visit(SqlGroupBy groupBy);

    public R visit(SqlColumn sqlColumn);

    public R visit(SqlFunctionAggregate sqlFunctionAggregate);

    public R visit(SqlFunctionAggregateGroupConcat sqlFunctionAggregateGroupConcat);

    public R visit(SqlFunctionScalar sqlFunctionScalar);

    public R visit(SqlFunctionScalarCase sqlFunctionScalarCase);

    public R visit(SqlFunctionScalarCast sqlFunctionScalarCast);

    public R visit(SqlFunctionScalarExtract sqlFunctionScalarExtract);

    public R visit(SqlLimit sqlLimit);

    public R visit(SqlLiteralBool sqlLiteralBool);

    public R visit(SqlLiteralDate sqlLiteralDate);

    public R visit(SqlLiteralDouble sqlLiteralDouble);

    public R visit(SqlLiteralExactnumeric sqlLiteralExactnumeric);

    public R visit(SqlLiteralNull sqlLiteralNull);

    public R visit(SqlLiteralString sqlLiteralString);

    public R visit(SqlLiteralTimestamp sqlLiteralTimestamp);

    public R visit(SqlLiteralTimestampUtc sqlLiteralTimestampUtc);

    public R visit(SqlLiteralInterval sqlLiteralInterval);

    public R visit(SqlOrderBy sqlOrderBy);

    public R visit(SqlPredicateAnd sqlPredicateAnd);

    public R visit(SqlPredicateBetween sqlPredicateBetween);

    public R visit(SqlPredicateEqual sqlPredicateEqual);

    public R visit(SqlPredicateInConstList sqlPredicateInConstList);

    public R visit(SqlPredicateLess sqlPredicateLess);

    public R visit(SqlPredicateLessEqual sqlPredicateLessEqual);

    public R visit(SqlPredicateLike sqlPredicateLike);

    public R visit(SqlPredicateLikeRegexp sqlPredicateLikeRegexp);

    public R visit(SqlPredicateNot sqlPredicateNot);

    public R visit(SqlPredicateNotEqual sqlPredicateNotEqual);

    public R visit(SqlPredicateOr sqlPredicateOr);

    public R visit(SqlPredicateIsNotNull sqlPredicateOr);

    public R visit(SqlPredicateIsNull sqlPredicateOr);

    public R visit(SqlTable sqlTable);

}
