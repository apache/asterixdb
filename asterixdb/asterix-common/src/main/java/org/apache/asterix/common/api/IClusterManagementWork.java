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

public interface IClusterManagementWork {

    enum WorkType {
        ADD_NODE,
        REMOVE_NODE
    }

    enum ClusterState {
        UNUSABLE, // one or more cluster partitions are inactive or max id resources have not been reported
        PENDING, // the metadata node has not yet joined & initialized
        RECOVERING, // global recovery has not yet completed
        ACTIVE, // cluster is ACTIVE and ready for requests
        SHUTTING_DOWN, // a shutdown request has been received, and is underway
        REBALANCE_REQUIRED // one or more datasets require rebalance before the cluster is usable
    }

    WorkType getClusterManagementWorkType();

    int getWorkId();

    IClusterEventsSubscriber getSourceSubscriber();
}
