package edu.uci.ics.asterix.algebra.operators.physical;

import java.util.List;

import edu.uci.ics.asterix.metadata.declared.AqlSourceId;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSource;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourcePropertiesProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.AbstractScanPOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.BroadcastPartitioningProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;

/**
 * Class that embodies the commonalities between access method physical operators.
 */
public abstract class IndexSearchPOperator extends AbstractScanPOperator {

    private final IDataSourceIndex<String, AqlSourceId> idx;
    private final boolean requiresBroadcast;

    public IndexSearchPOperator(IDataSourceIndex<String, AqlSourceId> idx, boolean requiresBroadcast) {
        this.idx = idx;
        this.requiresBroadcast = requiresBroadcast;
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        IDataSource<?> ds = idx.getDataSource();
        IDataSourcePropertiesProvider dspp = ds.getPropertiesProvider();
        AbstractScanOperator as = (AbstractScanOperator) op;
        deliveredProperties = dspp.computePropertiesVector(as.getVariables());
    }

    protected int[] getKeyIndexes(List<LogicalVariable> keyVarList, IOperatorSchema[] inputSchemas) {
        if (keyVarList == null) {
            return null;
        }
        int[] keyIndexes = new int[keyVarList.size()];
        for (int i = 0; i < keyVarList.size(); i++) {
            keyIndexes[i] = inputSchemas[0].findVariable(keyVarList.get(i));
        }
        return keyIndexes;
    }
    
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent) {
        if (requiresBroadcast) {
            StructuralPropertiesVector[] pv = new StructuralPropertiesVector[1];
            pv[0] = new StructuralPropertiesVector(new BroadcastPartitioningProperty(null), null);
            return new PhysicalRequirements(pv, IPartitioningRequirementsCoordinator.NO_COORDINATION);
        } else {
            return super.getRequiredPropertiesForChildren(op, reqdByParent);
        }
    }
}
