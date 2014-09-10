package edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteOperator.Kind;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.TokenizeOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class TokenizePOperator extends AbstractPhysicalOperator {

    private final List<LogicalVariable> primaryKeys;
    private final List<LogicalVariable> secondaryKeys;
    private final IDataSourceIndex<?, ?> dataSourceIndex;

    public TokenizePOperator(List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys,
            IDataSourceIndex<?, ?> dataSourceIndex) {
        this.primaryKeys = primaryKeys;
        this.secondaryKeys = secondaryKeys;
        this.dataSourceIndex = dataSourceIndex;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.TOKENIZE;
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent) {
        return emptyUnaryRequirements();
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        deliveredProperties = (StructuralPropertiesVector) op2.getDeliveredPhysicalProperties().clone();
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        TokenizeOperator tokenizeOp = (TokenizeOperator) op;
        if (tokenizeOp.getOperation() != Kind.INSERT || !tokenizeOp.isBulkload()) {
            throw new AlgebricksException("Tokenize Operator only works when bulk-loading data.");
        }

        IMetadataProvider mp = context.getMetadataProvider();
        IVariableTypeEnvironment typeEnv = context.getTypeEnvironment(op);
        JobSpecification spec = builder.getJobSpec();
        RecordDescriptor inputDesc = JobGenHelper.mkRecordDescriptor(
                context.getTypeEnvironment(op.getInputs().get(0).getValue()), inputSchemas[0], context);
        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> runtimeAndConstraints = mp.getTokenizerRuntime(
                dataSourceIndex, propagatedSchema, inputSchemas, typeEnv, primaryKeys, secondaryKeys, null, inputDesc,
                context, spec, true);
        builder.contributeHyracksOperator(tokenizeOp, runtimeAndConstraints.first);
        builder.contributeAlgebricksPartitionConstraint(runtimeAndConstraints.first, runtimeAndConstraints.second);
        ILogicalOperator src = tokenizeOp.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, tokenizeOp, 0);
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return false;
    }
}
