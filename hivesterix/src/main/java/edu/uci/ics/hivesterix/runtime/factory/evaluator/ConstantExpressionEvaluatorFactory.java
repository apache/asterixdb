package edu.uci.ics.hivesterix.runtime.factory.evaluator;

import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;

import edu.uci.ics.hivesterix.logical.expression.Schema;
import edu.uci.ics.hivesterix.runtime.evaluator.ConstantExpressionEvaluator;
import edu.uci.ics.hivesterix.runtime.evaluator.ExpressionTranslator;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;

public class ConstantExpressionEvaluatorFactory implements
		ICopyEvaluatorFactory {

	private static final long serialVersionUID = 1L;

	private ExprNodeConstantDesc expr;

	private Schema schema;

	public ConstantExpressionEvaluatorFactory(ILogicalExpression expression,
			Schema inputSchema, IVariableTypeEnvironment env)
			throws AlgebricksException {
		try {
			expr = (ExprNodeConstantDesc) ExpressionTranslator
					.getHiveExpression(expression, env);
		} catch (Exception e) {
			throw new AlgebricksException(e.getMessage());
		}
		schema = inputSchema;
	}

	public ICopyEvaluator createEvaluator(IDataOutputProvider output)
			throws AlgebricksException {
		return new ConstantExpressionEvaluator(expr,
				schema.toObjectInspector(), output);
	}

	public String toString() {
		return "constant expression evaluator factory: " + expr.toString();
	}

}
