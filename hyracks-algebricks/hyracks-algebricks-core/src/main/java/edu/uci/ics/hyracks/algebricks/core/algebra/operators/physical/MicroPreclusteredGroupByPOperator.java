package edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.AlgebricksPipeline;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.jobgen.impl.JobGenHelper;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.operators.aggreg.NestedPlansAccumulatingAggregatorFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.operators.group.MicroPreClusteredGroupRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAccumulatingAggregatorFactory;

public class MicroPreclusteredGroupByPOperator extends AbstractPreclusteredGroupByPOperator {

    public MicroPreclusteredGroupByPOperator(List<LogicalVariable> columnList) {
        super(columnList);
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.MICRO_PRE_CLUSTERED_GROUP_BY;
    }

    @Override
    public boolean isMicroOperator() {
        return true;
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        int keys[] = JobGenHelper.variablesToFieldIndexes(columnList, inputSchemas[0]);
        GroupByOperator gby = (GroupByOperator) op;
        int numFds = gby.getDecorList().size();
        int fdColumns[] = new int[numFds];
        IVariableTypeEnvironment env = context.getTypeEnvironment(op);
        int j = 0;
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gby.getDecorList()) {
            ILogicalExpression expr = p.second.getValue();
            if (expr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                throw new AlgebricksException("pre-sorted group-by expects variable references.");
            }
            VariableReferenceExpression v = (VariableReferenceExpression) expr;
            LogicalVariable decor = v.getVariableReference();
            fdColumns[j++] = inputSchemas[0].findVariable(decor);
        }
        // compile subplans and set the gby op. schema accordingly
        AlgebricksPipeline[] subplans = compileSubplans(inputSchemas[0], gby, opSchema, context);
        IAccumulatingAggregatorFactory aggregatorFactory = new NestedPlansAccumulatingAggregatorFactory(subplans, keys,
                fdColumns);

        IBinaryComparatorFactory[] comparatorFactories = JobGenHelper.variablesToAscBinaryComparatorFactories(
                columnList, env, context);
        RecordDescriptor recordDescriptor = JobGenHelper.mkRecordDescriptor(op, opSchema, context);
        RecordDescriptor inputRecordDesc = JobGenHelper.mkRecordDescriptor(op.getInputs().get(0).getValue(),
                inputSchemas[0], context);
        MicroPreClusteredGroupRuntimeFactory runtime = new MicroPreClusteredGroupRuntimeFactory(keys,
                comparatorFactories, aggregatorFactory, inputRecordDesc, recordDescriptor, null);
        builder.contributeMicroOperator(gby, runtime, recordDescriptor);
        ILogicalOperator src = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, op, 0);
    }

}
