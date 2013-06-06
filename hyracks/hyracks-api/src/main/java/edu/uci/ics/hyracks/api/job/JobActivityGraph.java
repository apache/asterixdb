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
package edu.uci.ics.hyracks.api.job;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.IActivity;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;

public class JobActivityGraph implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Map<ActivityId, IActivity> activityMap;

    private final Map<ConnectorDescriptorId, IConnectorDescriptor> connectorMap;

    private final Map<ConnectorDescriptorId, RecordDescriptor> connectorRecordDescriptorMap;

    private final Map<ActivityId, List<IConnectorDescriptor>> activityInputMap;

    private final Map<ActivityId, List<IConnectorDescriptor>> activityOutputMap;

    private final Map<ConnectorDescriptorId, Pair<Pair<IActivity, Integer>, Pair<IActivity, Integer>>> connectorActivityMap;

    private final Map<ActivityId, Set<ActivityId>> blocked2blockerMap;

    public JobActivityGraph() {
        activityMap = new HashMap<ActivityId, IActivity>();
        connectorMap = new HashMap<ConnectorDescriptorId, IConnectorDescriptor>();
        connectorRecordDescriptorMap = new HashMap<ConnectorDescriptorId, RecordDescriptor>();
        activityInputMap = new HashMap<ActivityId, List<IConnectorDescriptor>>();
        activityOutputMap = new HashMap<ActivityId, List<IConnectorDescriptor>>();
        connectorActivityMap = new HashMap<ConnectorDescriptorId, Pair<Pair<IActivity, Integer>, Pair<IActivity, Integer>>>();
        blocked2blockerMap = new HashMap<ActivityId, Set<ActivityId>>();
    }

    public Map<ActivityId, IActivity> getActivityMap() {
        return activityMap;
    }

    public Map<ConnectorDescriptorId, IConnectorDescriptor> getConnectorMap() {
        return connectorMap;
    }

    public Map<ConnectorDescriptorId, RecordDescriptor> getConnectorRecordDescriptorMap() {
        return connectorRecordDescriptorMap;
    }

    public Map<ActivityId, Set<ActivityId>> getBlocked2BlockerMap() {
        return blocked2blockerMap;
    }

    public Map<ActivityId, List<IConnectorDescriptor>> getActivityInputMap() {
        return activityInputMap;
    }

    public Map<ActivityId, List<IConnectorDescriptor>> getActivityOutputMap() {
        return activityOutputMap;
    }

    public Map<ConnectorDescriptorId, Pair<Pair<IActivity, Integer>, Pair<IActivity, Integer>>> getConnectorActivityMap() {
        return connectorActivityMap;
    }

    public ActivityId getConsumerActivity(ConnectorDescriptorId cdId) {
        Pair<Pair<IActivity, Integer>, Pair<IActivity, Integer>> connEdge = connectorActivityMap.get(cdId);
        return connEdge.getRight().getLeft().getActivityId();
    }

    public ActivityId getProducerActivity(ConnectorDescriptorId cdId) {
        Pair<Pair<IActivity, Integer>, Pair<IActivity, Integer>> connEdge = connectorActivityMap.get(cdId);
        return connEdge.getLeft().getLeft().getActivityId();
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("ActivityNodes: " + activityMap);
        buffer.append('\n');
        buffer.append("Blocker->Blocked: " + blocked2blockerMap);
        buffer.append('\n');
        return buffer.toString();
    }
}