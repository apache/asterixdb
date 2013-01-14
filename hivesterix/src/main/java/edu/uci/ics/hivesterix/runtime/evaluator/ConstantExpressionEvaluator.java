package edu.uci.ics.hivesterix.runtime.evaluator;

import org.apache.hadoop.hive.ql.exec.ExprNodeConstantEvaluator;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;

public class ConstantExpressionEvaluator extends AbstractExpressionEvaluator {

	public ConstantExpressionEvaluator(ExprNodeConstantDesc expr,
			ObjectInspector oi, IDataOutputProvider output)
			throws AlgebricksException {
		super(new ExprNodeConstantEvaluator(expr), oi, output);
	}
}
