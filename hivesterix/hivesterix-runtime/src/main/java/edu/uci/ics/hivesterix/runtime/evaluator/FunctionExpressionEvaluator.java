package edu.uci.ics.hivesterix.runtime.evaluator;

import org.apache.hadoop.hive.ql.exec.ExprNodeGenericFuncEvaluator;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;

public class FunctionExpressionEvaluator extends AbstractExpressionEvaluator {

    public FunctionExpressionEvaluator(ExprNodeGenericFuncDesc expr, ObjectInspector oi, IDataOutputProvider output)
            throws AlgebricksException {
        super(new ExprNodeGenericFuncEvaluator(expr), oi, output);
    }

}
