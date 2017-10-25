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

package org.apache.hyracks.api.rewriter.runtime;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.ActivityCluster;
import org.apache.hyracks.api.job.ActivityClusterGraph;
import org.apache.hyracks.api.job.ActivityClusterId;
import org.apache.hyracks.api.rewriter.OneToOneConnectedActivityCluster;

/**
 * This class can be used to execute a DAG of activities inside which
 * there are only one-to-one connectors.
 *
 * @author yingyib
 */
public class SuperActivity extends OneToOneConnectedActivityCluster implements IActivity {
    private static final long serialVersionUID = 1L;
    private final ActivityId activityId;

    public SuperActivity(ActivityClusterGraph acg, ActivityClusterId id, ActivityId activityId) {
        super(acg, id);
        this.activityId = activityId;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            final IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions)
            throws HyracksDataException {
        final Map<ActivityId, IActivity> startActivities = new HashMap<>();
        Map<ActivityId, IActivity> activities = getActivityMap();
        activities.forEach((key, value) -> {
            /*
             * extract start activities
             */
            List<IConnectorDescriptor> conns = getActivityInputMap().get(key);
            if (conns == null || conns.isEmpty()) {
                startActivities.put(key, value);
            }
        });

        /*
         * wrap a RecordDescriptorProvider for the super activity
         */
        IRecordDescriptorProvider wrappedRecDescProvider = new IRecordDescriptorProvider() {

            @Override
            public RecordDescriptor getInputRecordDescriptor(ActivityId aid, int inputIndex) {
                if (startActivities.get(aid) != null) {
                    /*
                     * if the activity is a start (input boundary) activity
                     */
                    int superActivityInputChannel = SuperActivity.this.getClusterInputIndex(Pair.of(aid, inputIndex));
                    if (superActivityInputChannel >= 0) {
                        return recordDescProvider.getInputRecordDescriptor(activityId, superActivityInputChannel);
                    }
                }
                if (SuperActivity.this.getActivityMap().get(aid) != null) {
                    /*
                     * if the activity is an internal activity of the super activity
                     */
                    IConnectorDescriptor conn = getActivityInputMap().get(aid).get(inputIndex);
                    return getConnectorRecordDescriptorMap().get(conn.getConnectorId());
                }

                /*
                 * the following is for the case where the activity is in other SuperActivities
                 */
                ActivityClusterGraph acg = SuperActivity.this.getActivityClusterGraph();
                for (Entry<ActivityClusterId, ActivityCluster> entry : acg.getActivityClusterMap().entrySet()) {
                    ActivityCluster ac = entry.getValue();
                    for (Entry<ActivityId, IActivity> saEntry : ac.getActivityMap().entrySet()) {
                        SuperActivity sa = (SuperActivity) saEntry.getValue();
                        if (sa.getActivityMap().get(aid) != null) {
                            List<IConnectorDescriptor> conns = sa.getActivityInputMap().get(aid);
                            if (conns != null && conns.size() >= inputIndex) {
                                IConnectorDescriptor conn = conns.get(inputIndex);
                                return sa.getConnectorRecordDescriptorMap().get(conn.getConnectorId());
                            } else {
                                int superActivityInputChannel = sa.getClusterInputIndex(Pair.of(aid, inputIndex));
                                if (superActivityInputChannel >= 0) {
                                    return recordDescProvider.getInputRecordDescriptor(sa.getActivityId(),
                                            superActivityInputChannel);
                                }
                            }
                        }
                    }
                }
                return null;
            }

            @Override
            public RecordDescriptor getOutputRecordDescriptor(ActivityId aid, int outputIndex) {
                /*
                 * if the activity is an output-boundary activity
                 */
                int superActivityOutputChannel = SuperActivity.this.getClusterOutputIndex(Pair.of(aid, outputIndex));
                if (superActivityOutputChannel >= 0) {
                    return recordDescProvider.getOutputRecordDescriptor(activityId, superActivityOutputChannel);
                }

                if (SuperActivity.this.getActivityMap().get(aid) != null) {
                    /*
                     * if the activity is an internal activity of the super activity
                     */
                    IConnectorDescriptor conn = getActivityOutputMap().get(aid).get(outputIndex);
                    return getConnectorRecordDescriptorMap().get(conn.getConnectorId());
                }

                /*
                 * the following is for the case where the activity is in other SuperActivities
                 */
                ActivityClusterGraph acg = SuperActivity.this.getActivityClusterGraph();
                for (Entry<ActivityClusterId, ActivityCluster> entry : acg.getActivityClusterMap().entrySet()) {
                    ActivityCluster ac = entry.getValue();
                    for (Entry<ActivityId, IActivity> saEntry : ac.getActivityMap().entrySet()) {
                        SuperActivity sa = (SuperActivity) saEntry.getValue();
                        if (sa.getActivityMap().get(aid) != null) {
                            List<IConnectorDescriptor> conns = sa.getActivityOutputMap().get(aid);
                            if (conns != null && conns.size() >= outputIndex) {
                                IConnectorDescriptor conn = conns.get(outputIndex);
                                return sa.getConnectorRecordDescriptorMap().get(conn.getConnectorId());
                            } else {
                                superActivityOutputChannel = sa.getClusterOutputIndex(Pair.of(aid, outputIndex));
                                if (superActivityOutputChannel >= 0) {
                                    return recordDescProvider.getOutputRecordDescriptor(sa.getActivityId(),
                                            superActivityOutputChannel);
                                }
                            }
                        }
                    }
                }
                return null;
            }

        };
        return new SuperActivityOperatorNodePushable(this, startActivities, ctx, wrappedRecDescProvider, partition,
                nPartitions);
    }

    @Override
    public ActivityId getActivityId() {
        return activityId;
    }

    @Override
    public String toString() {
        return getActivityMap().values().toString();
    }
}
