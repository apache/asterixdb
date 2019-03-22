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
package org.apache.asterix.common.api;

import java.util.Set;

import org.apache.hyracks.api.application.IClusterLifecycleListener;
import org.apache.hyracks.api.job.IJobLifecycleListener;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

public interface INodeJobTracker extends IJobLifecycleListener, IClusterLifecycleListener {

    /**
     * Gets node {@code nodeId} pending jobs. If the node is not active,
     * an empty set is returned.
     *
     * @param nodeId
     * @return unmodifiable set of the node pending jobs.
     */
    Set<JobId> getPendingJobs(String nodeId);

    /**
     * Gets the set of nodes that will participate in the execution
     * of the job. The nodes will include only nodes that are known
     * to this {@link INodeJobTracker}
     *
     * @param spec
     * @return The participating nodes in the job execution
     */
    Set<String> getJobParticipatingNodes(JobSpecification spec);
}
