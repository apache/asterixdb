package edu.uci.ics.hivesterix.runtime.factory.evaluator;

import org.apache.hadoop.hive.ql.plan.UDTFDesc;

import edu.uci.ics.hivesterix.logical.expression.Schema;
import edu.uci.ics.hivesterix.runtime.evaluator.ExpressionTranslator;
import edu.uci.ics.hivesterix.runtime.evaluator.UDTFFunctionEvaluator;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyUnnestingFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyUnnestingFunctionFactory;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;

public class UnnestingFunctionFactory implements ICopyUnnestingFunctionFactory {

	private static final long serialVersionUID = 1L;

	private UDTFDesc expr;

	private Schema inputSchema;

	private int[] columns;

	public UnnestingFunctionFactory(ILogicalExpression expression,
			Schema schema, IVariableTypeEnvironment env)
			throws AlgebricksException {
		try {
			expr = (UDTFDesc) ExpressionTranslator.getHiveExpression(
					expression, env);
		} catch (Exception e) {
			throw new AlgebricksException(e.getMessage());
		}
		inputSchema = schema;
	}

	@Override
	public ICopyUnnestingFunction createUnnestingFunction(
			IDataOutputProvider provider) throws AlgebricksException {
		return new UDTFFunctionEvaluator(expr, inputSchema, columns,
				provider.getDataOutput());
	}

}
