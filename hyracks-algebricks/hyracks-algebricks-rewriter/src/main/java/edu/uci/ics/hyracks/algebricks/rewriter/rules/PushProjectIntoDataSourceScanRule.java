package edu.uci.ics.hyracks.algebricks.rewriter.rules;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class PushProjectIntoDataSourceScanRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getInputs().size() <= 0)
            return false;
        AbstractLogicalOperator project = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        if (project.getOperatorTag() != LogicalOperatorTag.PROJECT)
            return false;
        AbstractLogicalOperator exchange = (AbstractLogicalOperator) project.getInputs().get(0).getValue();
        if (exchange.getOperatorTag() != LogicalOperatorTag.EXCHANGE)
            return false;
        AbstractLogicalOperator inputOp = (AbstractLogicalOperator) exchange.getInputs().get(0).getValue();
        if (inputOp.getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN)
            return false;
        DataSourceScanOperator scanOp = (DataSourceScanOperator) inputOp;
        ProjectOperator projectOp = (ProjectOperator) project;
        scanOp.addProjectVariables(projectOp.getVariables());
        if (op.getOperatorTag() != LogicalOperatorTag.EXCHANGE) {
            op.getInputs().set(0, project.getInputs().get(0));
        } else {
            op.getInputs().set(0, exchange.getInputs().get(0));
        }
        return true;
    }
}
