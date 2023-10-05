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

import org.apache.asterix.common.cluster.PartitioningProperties;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.runtime.utils.RuntimeUtils;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.file.FileRemoveOperatorDescriptor;

public class DataverseUtil {

    private DataverseUtil() {
    }

    public static JobSpecification dropDataverseJobSpec(Dataverse dataverse, MetadataProvider md) {
        PartitioningProperties pp = md.splitAndConstraints(dataverse.getDatabaseName(), dataverse.getDataverseName());
        return dropJobSpec(md, pp);
    }

    public static JobSpecification dropDatabaseJobSpec(String database, MetadataProvider md) {
        PartitioningProperties pp = md.splitAndConstraints(database);
        return dropJobSpec(md, pp);
    }

    private static JobSpecification dropJobSpec(MetadataProvider metadata, PartitioningProperties pp) {
        JobSpecification jobSpec = RuntimeUtils.createJobSpecification(metadata.getApplicationContext());
        FileRemoveOperatorDescriptor frod =
                new FileRemoveOperatorDescriptor(jobSpec, pp.getSplitsProvider(), false, pp.getComputeStorageMap());
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(jobSpec, frod, pp.getConstraints());
        jobSpec.addRoot(frod);
        return jobSpec;
    }
}
