package edu.uci.ics.asterix.aql.expression.visitor;

import edu.uci.ics.asterix.aql.expression.JoinClause;
import edu.uci.ics.asterix.aql.expression.MetaVariableClause;
import edu.uci.ics.asterix.aql.expression.MetaVariableExpr;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public interface IAqlPlusExpressionVisitor<R, T> extends IAqlExpressionVisitor<R, T> {
    R visitJoinClause(JoinClause c, T arg) throws AsterixException;

    R visitMetaVariableClause(MetaVariableClause c, T arg) throws AsterixException;

    R visitMetaVariableExpr(MetaVariableExpr v, T arg) throws AsterixException;
}
