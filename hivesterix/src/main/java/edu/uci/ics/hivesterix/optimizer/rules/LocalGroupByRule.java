package edu.uci.ics.hivesterix.optimizer.rules;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hivesterix.logical.plan.HiveOperatorAnnotations;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.OperatorAnnotations;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.OneToOneExchangePOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class LocalGroupByRule implements IAlgebraicRewriteRule {

	@Override
	public boolean rewritePre(Mutable<ILogicalOperator> opRef,
			IOptimizationContext context) throws AlgebricksException {
		return false;
	}

	@Override
	public boolean rewritePost(Mutable<ILogicalOperator> opRef,
			IOptimizationContext context) throws AlgebricksException {
		AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
		if (op.getOperatorTag() != LogicalOperatorTag.GROUP) {
			return false;
		}
		Boolean localGby = (Boolean) op.getAnnotations().get(
				HiveOperatorAnnotations.LOCAL_GROUP_BY);
		if (localGby != null && localGby.equals(Boolean.TRUE)) {
			Boolean hashGby = (Boolean) op.getAnnotations().get(
					OperatorAnnotations.USE_HASH_GROUP_BY);
			Boolean externalGby = (Boolean) op.getAnnotations().get(
					OperatorAnnotations.USE_EXTERNAL_GROUP_BY);
			if ((hashGby != null && (hashGby.equals(Boolean.TRUE)) || (externalGby != null && externalGby
					.equals(Boolean.TRUE)))) {
				reviseExchange(op);
			} else {
				ILogicalOperator child = op.getInputs().get(0).getValue();
				AbstractLogicalOperator childOp = (AbstractLogicalOperator) child;
				while (child.getInputs().size() > 0) {
					if (childOp.getOperatorTag() == LogicalOperatorTag.ORDER)
						break;
					else {
						child = child.getInputs().get(0).getValue();
						childOp = (AbstractLogicalOperator) child;
					}
				}
				if (childOp.getOperatorTag() == LogicalOperatorTag.ORDER)
					reviseExchange(childOp);
			}
			return true;
		}
		return false;
	}

	private void reviseExchange(AbstractLogicalOperator op) {
		ExchangeOperator exchange = (ExchangeOperator) op.getInputs().get(0)
				.getValue();
		IPhysicalOperator physicalOp = exchange.getPhysicalOperator();
		if (physicalOp.getOperatorTag() == PhysicalOperatorTag.HASH_PARTITION_EXCHANGE) {
			exchange.setPhysicalOperator(new OneToOneExchangePOperator());
		}
	}

}
