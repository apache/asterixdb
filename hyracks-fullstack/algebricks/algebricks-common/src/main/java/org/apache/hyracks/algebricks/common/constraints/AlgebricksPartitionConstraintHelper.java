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
package org.apache.hyracks.algebricks.common.constraints;

import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.job.JobSpecification;

public class AlgebricksPartitionConstraintHelper {

    public static void setPartitionConstraintInJobSpec(JobSpecification jobSpec, IOperatorDescriptor opDesc,
            AlgebricksPartitionConstraint apc) {
        switch (apc.getPartitionConstraintType()) {
            case ABSOLUTE: {
                AlgebricksAbsolutePartitionConstraint absPc = (AlgebricksAbsolutePartitionConstraint) apc;
                PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, opDesc, absPc.getLocations());
                break;
            }
            case COUNT: {
                AlgebricksCountPartitionConstraint cntPc = (AlgebricksCountPartitionConstraint) apc;
                PartitionConstraintHelper.addPartitionCountConstraint(jobSpec, opDesc, cntPc.getCount());
                break;
            }
            default: {
                throw new IllegalStateException();
            }
        }
    }

    public static int getPartitionCount(AlgebricksPartitionConstraint partitionConstraint) {
        switch (partitionConstraint.getPartitionConstraintType()) {
            case COUNT: {
                AlgebricksCountPartitionConstraint pcc = (AlgebricksCountPartitionConstraint) partitionConstraint;
                return pcc.getCount();
            }
            case ABSOLUTE: {
                AlgebricksAbsolutePartitionConstraint epc = (AlgebricksAbsolutePartitionConstraint) partitionConstraint;
                return epc.getLocations().length;
            }
            default: {
                throw new IllegalStateException();
            }
        }
    }

}
