package edu.uci.ics.hivesterix.runtime.factory.evaluator;

import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;

import edu.uci.ics.hivesterix.logical.expression.Schema;
import edu.uci.ics.hivesterix.runtime.evaluator.ExpressionTranslator;
import edu.uci.ics.hivesterix.runtime.evaluator.FieldExpressionEvaluator;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;

public class FieldExpressionEvaluatorFactory implements ICopyEvaluatorFactory {
	private static final long serialVersionUID = 1L;

	private ExprNodeFieldDesc expr;

	private Schema inputSchema;

	public FieldExpressionEvaluatorFactory(ILogicalExpression expression,
			Schema schema, IVariableTypeEnvironment env)
			throws AlgebricksException {
		try {
			expr = (ExprNodeFieldDesc) ExpressionTranslator.getHiveExpression(
					expression, env);
		} catch (Exception e) {
			throw new AlgebricksException(e.getMessage());
		}
		inputSchema = schema;
	}

	public ICopyEvaluator createEvaluator(IDataOutputProvider output)
			throws AlgebricksException {
		return new FieldExpressionEvaluator(expr,
				inputSchema.toObjectInspector(), output);
	}

	public String toString() {
		return "field access expression evaluator factory: " + expr.toString();
	}

}
