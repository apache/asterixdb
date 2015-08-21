/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.asterix.file;

import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.file.FileRemoveOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;

public class DataverseOperations {
    public static JobSpecification createDropDataverseJobSpec(Dataverse dataverse, AqlMetadataProvider metadata) {
        JobSpecification jobSpec = JobSpecificationUtils.createJobSpecification();
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = metadata
                .splitProviderAndPartitionConstraintsForDataverse(dataverse.getDataverseName());
        FileRemoveOperatorDescriptor frod = new FileRemoveOperatorDescriptor(jobSpec, splitsAndConstraint.first);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(jobSpec, frod, splitsAndConstraint.second);
        jobSpec.addRoot(frod);
        return jobSpec;
    }
}
