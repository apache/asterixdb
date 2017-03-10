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

import java.io.Serializable;

import org.apache.hyracks.api.context.ICCContext;
import org.apache.hyracks.api.job.IJobLifecycleListener;

/**
 * Service Context at the Cluster Controller for an application.
 */
public interface ICCServiceContext extends IServiceContext {
    /**
     * Sets the state that must be distributed by the infrastructure
     * to all the {@link org.apache.hyracks.api.application.INCServiceContext}.
     * Any state set by calling this method in the {@link ICCApplication#start(ICCServiceContext, String[])} call
     * is made available to all the {@link INCServiceContext} objects at each Node Controller.
     *
     * @param state
     *            The distributed state
     */
    void setDistributedState(Serializable state);

    /**
     * A listener that listens to Job Lifecycle events at the Cluster
     * Controller.
     *
     * @param jobLifecycleListener
     */
    void addJobLifecycleListener(IJobLifecycleListener jobLifecycleListener);

    /**
     * A listener that listens to Cluster Lifecycle events at the Cluster
     * Controller.
     *
     * @param clusterLifecycleListener
     */
    void addClusterLifecycleListener(IClusterLifecycleListener clusterLifecycleListener);

    /**
     * Get the Cluster Controller Context.
     *
     * @return The Cluster Controller Context.
     */
    ICCContext getCCContext();

}
