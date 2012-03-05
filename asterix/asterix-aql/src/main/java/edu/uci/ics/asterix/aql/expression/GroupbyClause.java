package edu.uci.ics.asterix.aql.expression;

import java.util.List;

import edu.uci.ics.asterix.aql.base.Clause;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class GroupbyClause implements Clause {

    private List<GbyVariableExpressionPair> gbyPairList;
    private List<GbyVariableExpressionPair> decorPairList;
    private List<VariableExpr> withVarList;
    private boolean hashGroupByHint;

    public GroupbyClause() {
    }

    public GroupbyClause(List<GbyVariableExpressionPair> gbyPairList, List<GbyVariableExpressionPair> decorPairList,
            List<VariableExpr> withVarList, boolean hashGroupByHint) {
        this.gbyPairList = gbyPairList;
        this.setDecorPairList(decorPairList);
        this.withVarList = withVarList;
        this.hashGroupByHint = hashGroupByHint;
    }

    public List<GbyVariableExpressionPair> getGbyPairList() {
        return gbyPairList;
    }

    public void setGbyPairList(List<GbyVariableExpressionPair> vePairList) {
        this.gbyPairList = vePairList;
    }

    public List<VariableExpr> getWithVarList() {
        return withVarList;
    }

    public void setWithVarList(List<VariableExpr> withVarList) {
        this.withVarList = withVarList;
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.GROUP_BY_CLAUSE;
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitGroupbyClause(this, arg);
    }

    public void setDecorPairList(List<GbyVariableExpressionPair> decorPairList) {
        this.decorPairList = decorPairList;
    }

    public List<GbyVariableExpressionPair> getDecorPairList() {
        return decorPairList;
    }

    public void setHashGroupByHint(boolean hashGroupByHint) {
        this.hashGroupByHint = hashGroupByHint;
    }

    public boolean hasHashGroupByHint() {
        return hashGroupByHint;
    }
}
