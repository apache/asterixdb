package edu.uci.ics.hivesterix.logical.plan.visitor;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.plan.LimitDesc;

import edu.uci.ics.hivesterix.logical.expression.HivesterixConstantValue;
import edu.uci.ics.hivesterix.logical.plan.visitor.base.DefaultVisitor;
import edu.uci.ics.hivesterix.logical.plan.visitor.base.Translator;
import edu.uci.ics.hivesterix.runtime.jobgen.Schema;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;

public class LimitVisitor extends DefaultVisitor {

    @Override
    public Mutable<ILogicalOperator> visit(LimitOperator operator,
            Mutable<ILogicalOperator> AlgebricksParentOperatorRef, Translator t) {
        Schema currentSchema = t.generateInputSchema(operator.getParentOperators().get(0));

        LimitDesc desc = (LimitDesc) operator.getConf();
        int limit = desc.getLimit();
        Integer limitValue = new Integer(limit);

        ILogicalExpression expr = new ConstantExpression(new HivesterixConstantValue(limitValue));
        ILogicalOperator currentOperator = new edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.LimitOperator(
                expr, true);
        currentOperator.getInputs().add(AlgebricksParentOperatorRef);

        operator.setSchema(operator.getParentOperators().get(0).getSchema());
        List<LogicalVariable> latestOutputSchema = t.getVariablesFromSchema(currentSchema);
        t.rewriteOperatorOutputSchema(latestOutputSchema, operator);
        return new MutableObject<ILogicalOperator>(currentOperator);
    }

}
