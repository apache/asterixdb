/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.utils;

import org.apache.asterix.app.resource.RequiredCapacityVisitor;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.api.job.resource.ClusterCapacity;
import org.apache.hyracks.api.job.resource.IClusterCapacity;

public class ResourceUtils {

    private ResourceUtils() {
    }

    /**
     * Calculates the required cluster capacity from a given query plan, the computation locations,
     * the operator memory budgets, and frame size.
     *
     * @param plan,
     *            a given query plan.
     * @param computationLocations,
     *            the partitions for computation.
     * @param sortFrameLimit,
     *            the frame limit for one sorter partition.
     * @param groupFrameLimit,
     *            the frame limit for one group-by partition.
     * @param joinFrameLimit
     *            the frame limit for one joiner partition.
     * @param frameSize
     *            the frame size used in query execution.
     * @return the required cluster capacity for executing the query.
     * @throws AlgebricksException
     *             if the query plan is malformed.
     */
    public static IClusterCapacity getRequiredCompacity(ILogicalPlan plan,
            AlgebricksAbsolutePartitionConstraint computationLocations, int sortFrameLimit, int groupFrameLimit,
            int joinFrameLimit, int frameSize)
            throws AlgebricksException {
        // Creates a cluster capacity visitor.
        IClusterCapacity clusterCapacity = new ClusterCapacity();
        RequiredCapacityVisitor visitor = new RequiredCapacityVisitor(computationLocations.getLocations().length,
                sortFrameLimit, groupFrameLimit, joinFrameLimit, frameSize, clusterCapacity);

        // There could be only one root operator for a top-level query plan.
        ILogicalOperator rootOp = plan.getRoots().get(0).getValue();
        rootOp.accept(visitor, null);
        return clusterCapacity;
    }

}
