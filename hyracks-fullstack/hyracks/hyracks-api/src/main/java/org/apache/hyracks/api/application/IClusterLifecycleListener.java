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
package org.apache.hyracks.api.application;

import java.util.Collection;
import java.util.Map;

import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.exceptions.HyracksException;

/**
 * A listener interface for providing notification call backs to events such as a Node Controller joining/leaving the
 * cluster.
 */
public interface IClusterLifecycleListener {

    public enum ClusterEventType {
        NODE_JOIN,
        NODE_FAILURE,
        NODE_SHUTTING_DOWN //node shutting down gracefully
    }

    /**
     * @param nodeId
     *            A unique identifier of a Node Controller
     * @param ncConfiguration
     */
    public void notifyNodeJoin(String nodeId, Map<IOption, Object> ncConfiguration) throws HyracksException;

    /**
     * @param deadNodeIds
     *            A set of Node Controller Ids that have left the cluster. The set is not cumulative.
     */
    public void notifyNodeFailure(Collection<String> deadNodeIds) throws HyracksException;

}
