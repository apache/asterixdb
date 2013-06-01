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

import java.util.Map;
import java.util.Set;

/**
 * A listener interface for providing notification call backs to events such as a Node Controller joining/leaving the cluster.
 */
public interface IClusterLifecycleListener {

    /**
     * @param nodeId
     *            A unique identifier of a Node Controller
     * @param ncConfig
     *            A map containing the set of configuration parameters that were used to start the Node Controller
     */
    public void notifyNodeJoin(String nodeId, Map<String, String> ncConfiguration);

    /**
     * @param deadNodeIds
     *            A set of Node Controller Ids that have left the cluster. The set is not cumulative.
     */
    public void notifyNodeFailure(Set<String> deadNodeIds);

}
