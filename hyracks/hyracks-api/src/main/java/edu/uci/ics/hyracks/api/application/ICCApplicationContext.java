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
package edu.uci.ics.hyracks.api.application;

import java.io.Serializable;

import edu.uci.ics.hyracks.api.context.ICCContext;
import edu.uci.ics.hyracks.api.job.IJobLifecycleListener;

/**
 * Application Context at the Cluster Controller for an application.
 * 
 * @author vinayakb
 */
public interface ICCApplicationContext extends IApplicationContext {
    /**
     * Sets the state that must be distributed by the infrastructure to all the
     * NC application contexts. Any state set by calling this method in the {@link ICCApplicationEntryPoint#start(ICCApplicationContext, String[])} call is made available to all the {@link INCApplicationContext} objects
     * at each Node Controller. The state is then available to be inspected by
     * the application at the NC during or after the {@link INCBootstrap#start()} call.
     * 
     * @param state
     *            The distributed state
     */
    public void setDistributedState(Serializable state);

    /**
     * A listener that listens to Job Lifecycle events at the Cluster
     * Controller.
     * 
     * @param jobLifecycleListener
     */
    public void addJobLifecycleListener(IJobLifecycleListener jobLifecycleListener);

    /**
     * A listener that listens to Cluster Lifecycle events at the Cluster
     * Controller.
     * 
     * @param jobLifecycleListener
     */
    public void addClusterLifecycleListener(IClusterLifecycleListener clusterLifecycleListener);

    /**
     * Get the Cluster Controller Context.
     * 
     * @return The Cluster Controller Context.
     */
    public ICCContext getCCContext();
}