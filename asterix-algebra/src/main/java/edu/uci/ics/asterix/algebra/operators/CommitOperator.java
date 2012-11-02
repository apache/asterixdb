package edu.uci.ics.asterix.algebra.operators;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractExtensibleLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorExtension;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;

public class CommitOperator extends AbstractExtensibleLogicalOperator {

    @Override
    public boolean isMap() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public IOperatorExtension newInstance() {
        return new CommitOperator();
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform transform) throws AlgebricksException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getOperatorName() {
        return "commit";
    }
}
