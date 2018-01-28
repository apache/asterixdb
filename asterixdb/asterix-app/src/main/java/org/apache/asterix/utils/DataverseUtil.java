/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.utils;

import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.runtime.utils.RuntimeUtils;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.file.FileRemoveOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;

public class DataverseUtil {

    private DataverseUtil() {
    }

    public static JobSpecification dropDataverseJobSpec(Dataverse dataverse, MetadataProvider metadata) {
        JobSpecification jobSpec = RuntimeUtils.createJobSpecification(metadata.getApplicationContext());
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                metadata.splitAndConstraints(dataverse.getDataverseName());
        FileRemoveOperatorDescriptor frod = new FileRemoveOperatorDescriptor(jobSpec, splitsAndConstraint.first, false);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(jobSpec, frod, splitsAndConstraint.second);
        jobSpec.addRoot(frod);
        return jobSpec;
    }
}
