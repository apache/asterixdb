package edu.uci.ics.asterix.aql.base;

public interface Clause extends IAqlExpression {
    public ClauseType getClauseType();

    public enum ClauseType {
        FOR_CLAUSE,
        LET_CLAUSE,
        WHERE_CLAUSE,
        ORDER_BY_CLAUSE,
        LIMIT_CLAUSE,
        DIE_CLAUSE,
        GROUP_BY_CLAUSE,
        DISTINCT_BY_CLAUSE,
        UPDATE_CLAUSE
    }

}
