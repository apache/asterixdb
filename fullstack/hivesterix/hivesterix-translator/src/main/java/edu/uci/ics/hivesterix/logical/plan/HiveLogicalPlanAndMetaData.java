package edu.uci.ics.hivesterix.logical.plan;

import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlanAndMetadata;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class HiveLogicalPlanAndMetaData implements ILogicalPlanAndMetadata {

    IMetadataProvider metadata;
    ILogicalPlan plan;

    public HiveLogicalPlanAndMetaData(ILogicalPlan plan, IMetadataProvider metadata) {
        this.plan = plan;
        this.metadata = metadata;
    }

    @Override
    public IMetadataProvider getMetadataProvider() {
        return metadata;
    }

    @Override
    public ILogicalPlan getPlan() {
        return plan;
    }

    @Override
    public AlgebricksPartitionConstraint getClusterLocations() {
        // TODO Auto-generated method stub
        return null;
    }

}
