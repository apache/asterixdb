package edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSource;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteOperator.Kind;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.jobgen.impl.JobGenHelper;
import edu.uci.ics.hyracks.algebricks.core.api.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;

@SuppressWarnings("rawtypes")
public class InsertDeletePOperator extends AbstractPhysicalOperator {

    private LogicalVariable payload;
    private List<LogicalVariable> keys;
    private IDataSource<?> dataSource;

    public InsertDeletePOperator(LogicalVariable payload, List<LogicalVariable> keys, IDataSource dataSource) {
        this.payload = payload;
        this.keys = keys;
        this.dataSource = dataSource;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.INSERT_DELETE;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op.getInputs().get(0).getOperator();
        deliveredProperties = (StructuralPropertiesVector) op2.getDeliveredPhysicalProperties().clone();
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent) {
        List<LogicalVariable> scanVariables = new ArrayList<LogicalVariable>();
        scanVariables.addAll(keys);
        scanVariables.add(new LogicalVariable(-1));
        IPhysicalPropertiesVector r = dataSource.getPropertiesProvider().computePropertiesVector(scanVariables);
        IPhysicalPropertiesVector[] requirements = new IPhysicalPropertiesVector[1];
        requirements[0] = r;
        return new PhysicalRequirements(requirements, IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        InsertDeleteOperator insertDeleteOp = (InsertDeleteOperator) op;
        IMetadataProvider mp = context.getMetadataProvider();
        JobSpecification spec = builder.getJobSpec();
        RecordDescriptor inputDesc = JobGenHelper.mkRecordDescriptor(op.getInputs().get(0).getOperator(),
                inputSchemas[0], context);

        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> runtimeAndConstraints = null;
        if (insertDeleteOp.getOperation() == Kind.INSERT)
            runtimeAndConstraints = mp.getInsertRuntime(dataSource, propagatedSchema, keys, payload, inputDesc,
                    context, spec);
        else
            runtimeAndConstraints = mp.getDeleteRuntime(dataSource, propagatedSchema, keys, payload, inputDesc,
                    context, spec);

        builder.contributeHyracksOperator(insertDeleteOp, runtimeAndConstraints.first);
        builder.contributeAlgebricksPartitionConstraint(runtimeAndConstraints.first, runtimeAndConstraints.second);
        ILogicalOperator src = insertDeleteOp.getInputs().get(0).getOperator();
        builder.contributeGraphEdge(src, 0, insertDeleteOp, 0);
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

}
