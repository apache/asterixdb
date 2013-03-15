package edu.uci.ics.hivesterix.runtime.evaluator;

import org.apache.hadoop.hive.ql.exec.ExprNodeNullEvaluator;
import org.apache.hadoop.hive.ql.plan.ExprNodeNullDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;

public class NullExpressionEvaluator extends AbstractExpressionEvaluator {

    public NullExpressionEvaluator(ExprNodeNullDesc expr, ObjectInspector oi, IDataOutputProvider output)
            throws AlgebricksException {
        super(new ExprNodeNullEvaluator(expr), oi, output);
    }
}
