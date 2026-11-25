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
package org.apache.asterix.common.cluster;

import org.apache.asterix.common.api.IClusterManagementWork.ClusterState;

/**
 * Told of every change of the cluster state, for example to keep statistics about it.
 */
@FunctionalInterface
public interface IClusterStateChangeListener {

    /**
     * Called after the cluster state has changed from {@code from} to {@code to}, never with
     * {@code from == to}. Called on the thread that changed the state while it holds the
     * {@link IClusterStateManager}'s monitor, so calls arrive in the order the changes were made; an implementation
     * must therefore return quickly, must not block, and must not call back into the {@link IClusterStateManager}.
     *
     * @param from
     *            the state the cluster was in
     * @param to
     *            the state the cluster is now in
     */
    void stateChanged(ClusterState from, ClusterState to);
}
