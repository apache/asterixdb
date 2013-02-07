package edu.uci.ics.hivesterix.runtime.evaluator;

import org.apache.hadoop.hive.ql.exec.ExprNodeFieldEvaluator;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;

public class FieldExpressionEvaluator extends AbstractExpressionEvaluator {

	public FieldExpressionEvaluator(ExprNodeFieldDesc expr, ObjectInspector oi,
			IDataOutputProvider output) throws AlgebricksException {
		super(new ExprNodeFieldEvaluator(expr), oi, output);
	}

}