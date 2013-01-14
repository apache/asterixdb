package edu.uci.ics.hivesterix.logical.plan.visitor;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;

import edu.uci.ics.hivesterix.logical.expression.Schema;
import edu.uci.ics.hivesterix.logical.plan.visitor.base.DefaultVisitor;
import edu.uci.ics.hivesterix.logical.plan.visitor.base.Translator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;

public class FilterVisitor extends DefaultVisitor {

	@Override
	public Mutable<ILogicalOperator> visit(FilterOperator operator,
			Mutable<ILogicalOperator> AlgebricksParentOperatorRef, Translator t) {
		Schema currentSchema = t.generateInputSchema(operator
				.getParentOperators().get(0));

		FilterDesc desc = (FilterDesc) operator.getConf();
		ExprNodeDesc predicate = desc.getPredicate();
		t.rewriteExpression(predicate);

		Mutable<ILogicalExpression> exprs = t.translateScalarFucntion(desc
				.getPredicate());
		ILogicalOperator currentOperator = new SelectOperator(exprs);
		currentOperator.getInputs().add(AlgebricksParentOperatorRef);

		// populate the schema from upstream operator
		operator.setSchema(operator.getParentOperators().get(0).getSchema());
		List<LogicalVariable> latestOutputSchema = t
				.getVariablesFromSchema(currentSchema);
		t.rewriteOperatorOutputSchema(latestOutputSchema, operator);
		return new MutableObject<ILogicalOperator>(currentOperator);
	}

}
