package edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import edu.uci.ics.hyracks.algebricks.core.algebra.typing.ITypingContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.typing.NonPropagatingTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class AggregateOperator extends AbstractAssignOperator {

    // private ArrayList<AggregateFunctionCallExpression> expressions;
    // TODO type safe list of expressions
    private List<Mutable<ILogicalExpression>> mergeExpressions;

    public AggregateOperator(List<LogicalVariable> variables, List<Mutable<ILogicalExpression>> expressions) {
        super(variables, expressions);
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.AGGREGATE;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitAggregateOperator(this, arg);
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return new VariablePropagationPolicy() {

            @Override
            public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources)
                    throws AlgebricksException {
                for (LogicalVariable v : variables) {
                    target.addVariable(v);
                }
            }
        };
    }

    @Override
    public boolean isMap() {
        return false;
    }

    @Override
    public void recomputeSchema() {
        schema = new ArrayList<LogicalVariable>();
        schema.addAll(variables);
    }

    public void setMergeExpressions(List<Mutable<ILogicalExpression>> merges) {
        mergeExpressions = merges;
    }

    public List<Mutable<ILogicalExpression>> getMergeExpressions() {
        return mergeExpressions;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        IVariableTypeEnvironment env = new NonPropagatingTypeEnvironment(ctx.getExpressionTypeComputer(),
                ctx.getMetadataProvider());
        IVariableTypeEnvironment env2 = ctx.getOutputTypeEnvironment(inputs.get(0).getValue());
        int n = variables.size();
        for (int i = 0; i < n; i++) {
            Object t = ctx.getExpressionTypeComputer().getType(expressions.get(i).getValue(),
                    ctx.getMetadataProvider(), env2);
            env.setVarType(variables.get(i), t);
        }
        return env;
    }
}
