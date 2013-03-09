package edu.uci.ics.hivesterix.runtime.evaluator;

import org.apache.hadoop.hive.ql.exec.ExprNodeColumnEvaluator;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;

public class ColumnExpressionEvaluator extends AbstractExpressionEvaluator {

    public ColumnExpressionEvaluator(ExprNodeColumnDesc expr, ObjectInspector oi, IDataOutputProvider output)
            throws AlgebricksException {
        super(new ExprNodeColumnEvaluator(expr), oi, output);
    }

}
