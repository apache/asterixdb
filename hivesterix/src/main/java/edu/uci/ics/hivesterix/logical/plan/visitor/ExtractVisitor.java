package edu.uci.ics.hivesterix.logical.plan.visitor;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hadoop.hive.ql.exec.ExtractOperator;

import edu.uci.ics.hivesterix.logical.expression.Schema;
import edu.uci.ics.hivesterix.logical.plan.visitor.base.DefaultVisitor;
import edu.uci.ics.hivesterix.logical.plan.visitor.base.Translator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;

public class ExtractVisitor extends DefaultVisitor {

	@Override
	public Mutable<ILogicalOperator> visit(ExtractOperator operator,
			Mutable<ILogicalOperator> AlgebricksParentOperatorRef, Translator t) {
		Schema currentSchema = t.generateInputSchema(operator
				.getParentOperators().get(0));
		operator.setSchema(operator.getParentOperators().get(0).getSchema());
		List<LogicalVariable> latestOutputSchema = t
				.getVariablesFromSchema(currentSchema);
		t.rewriteOperatorOutputSchema(latestOutputSchema, operator);
		return null;
	}

}
