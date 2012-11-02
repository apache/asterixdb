package edu.uci.ics.asterix.optimizer.rules;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.algebra.operators.CommitOperator;
import edu.uci.ics.asterix.algebra.operators.physical.CommitPOperator;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ExtensionOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SinkOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class ReplaceSinkOpWithCommitOpRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {

        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.SINK) {
            return false;
        }
        SinkOperator sinkOperator = (SinkOperator) op;

        List<Mutable<ILogicalExpression>> primaryKeyExprs = null;
        AbstractLogicalOperator descendantOp = (AbstractLogicalOperator) sinkOperator.getInputs().get(0).getValue();
        while (descendantOp != null) {
            if (descendantOp.getOperatorTag() == LogicalOperatorTag.INDEX_INSERT_DELETE) {
                IndexInsertDeleteOperator indexInsertDeleteOperator = (IndexInsertDeleteOperator) descendantOp;
                primaryKeyExprs = indexInsertDeleteOperator.getPrimaryKeyExpressions();
                break;
            } else if (descendantOp.getOperatorTag() == LogicalOperatorTag.INSERT_DELETE) {
                InsertDeleteOperator insertDeleteOperator = (InsertDeleteOperator) descendantOp;
                primaryKeyExprs = insertDeleteOperator.getPrimaryKeyExpressions();
                break;
            }
            descendantOp = (AbstractLogicalOperator) descendantOp.getInputs().get(0).getValue();
        }
        
        if (primaryKeyExprs != null) {
            //TODO
            //set the proper parameters for the constructor of the CommitOperator.
            //also, set the CommitPOperator, too.
            CommitOperator commitOperator = new CommitOperator();
            ExtensionOperator extensionOperator = new ExtensionOperator(commitOperator);
            CommitPOperator commitPOperator = new CommitPOperator();
            commitOperator.setPhysicalOperator(commitPOperator);
            extensionOperator.setPhysicalOperator(commitPOperator);
            extensionOperator.getInputs().add(sinkOperator.getInputs().get(0));
            context.computeAndSetTypeEnvironmentForOperator(extensionOperator);
            opRef.setValue(extensionOperator);
        }

        return true;
    }

}
