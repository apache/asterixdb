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

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.apache.asterix.common.api.IClusterManagementWork.ClusterState;

public interface IClusterEventsSubscriber {

    /**
     * @param deadNodeIds
     * @return set of work to execute as a result of this node failure
     */
    default Set<IClusterManagementWork> notifyNodeFailure(Collection<String> deadNodeIds) {
        // default is no-op
        return Collections.emptySet();
    }

    /**
     * @param joinedNodeId
     * @return set of work to execute as a result of this node join
     */
    default Set<IClusterManagementWork> notifyNodeJoin(String joinedNodeId) {
        // default is no-op
        return Collections.emptySet();
    }

    /**
     * @param response
     */
    default void notifyRequestCompletion(IClusterManagementWorkResponse response) {
        // default is no-op
    }

    /**
     * @param previousState
     * @param newState
     */
    default void notifyStateChange(ClusterState newState) {
        // default is no-op
    }

}
