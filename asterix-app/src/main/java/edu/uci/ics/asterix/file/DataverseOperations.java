package edu.uci.ics.asterix.file;

import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.entities.Dataverse;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.FileRemoveOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;

public class DataverseOperations {
    public static JobSpecification createDropDataverseJobSpec(Dataverse dataverse, AqlMetadataProvider metadata) {
        JobSpecification jobSpec = new JobSpecification();
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = metadata
                .splitProviderAndPartitionConstraintsForDataverse(dataverse.getDataverseName());
        FileRemoveOperatorDescriptor frod = new FileRemoveOperatorDescriptor(jobSpec, splitsAndConstraint.first);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(jobSpec, frod, splitsAndConstraint.second);
        jobSpec.addRoot(frod);
        return jobSpec;
    }
}
