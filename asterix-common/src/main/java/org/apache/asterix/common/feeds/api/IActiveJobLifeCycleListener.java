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
package org.apache.asterix.common.feeds.api;

import java.util.Set;

import org.apache.asterix.common.api.IClusterEventsSubscriber;
import org.apache.asterix.common.api.IClusterManagementWork;
import org.apache.asterix.common.api.IClusterManagementWork.ClusterState;
import org.apache.asterix.common.api.IClusterManagementWorkResponse;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.IActivityClusterGraphGeneratorFactory;
import org.apache.hyracks.api.job.IJobLifecycleListener;
import org.apache.hyracks.api.job.JobId;

public class IActiveJobLifeCycleListener implements IJobLifecycleListener, IClusterEventsSubscriber {

    public enum ConnectionLocation {
        SOURCE_FEED_INTAKE_STAGE,
        SOURCE_FEED_COMPUTE_STAGE
    }

    @Override
    public Set<IClusterManagementWork> notifyNodeFailure(Set<String> deadNodeIds) {
        return null;
    }

    @Override
    public Set<IClusterManagementWork> notifyNodeJoin(String joinedNodeId) {
        return null;
    }

    @Override
    public void notifyRequestCompletion(IClusterManagementWorkResponse response) {
    }

    @Override
    public void notifyStateChange(ClusterState previousState, ClusterState newState) {
    }

    @Override
    public void notifyJobCreation(JobId jobId, IActivityClusterGraphGeneratorFactory acggf) throws HyracksException {
    }

    @Override
    public void notifyJobStart(JobId jobId) throws HyracksException {

    }

    @Override
    public void notifyJobFinish(JobId jobId) throws HyracksException {

    }

}
