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

package org.apache.hyracks.hdfs2.scheduler;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.topology.ClusterTopology;
import org.apache.hyracks.hdfs.api.INcCollectionBuilder;

/**
 * The scheduler conduct data-local scheduling for data reading on HDFS.
 * This class works for Hadoop new API.
 */
@SuppressWarnings("deprecation")
public class Scheduler {

    private org.apache.hyracks.hdfs.scheduler.Scheduler scheduler;

    /**
     * The constructor of the scheduler
     *
     * @param ncNameToNcInfos
     * @throws HyracksException
     */
    public Scheduler(String ipAddress, int port) throws HyracksException {
        scheduler = new org.apache.hyracks.hdfs.scheduler.Scheduler(ipAddress, port);
    }

    /**
     * The constructor of the scheduler.
     *
     * @param ncNameToNcInfos
     *            the mapping from nc names to nc infos
     * @throws HyracksException
     */
    public Scheduler(Map<String, NodeControllerInfo> ncNameToNcInfos) throws HyracksException {
        scheduler = new org.apache.hyracks.hdfs.scheduler.Scheduler(ncNameToNcInfos);
    }

    /**
     * The constructor of the scheduler.
     *
     * @param ncNameToNcInfos
     *            the mapping from nc names to nc infos
     * @param topology
     *            the hyracks cluster toplogy
     * @throws HyracksException
     */
    public Scheduler(Map<String, NodeControllerInfo> ncNameToNcInfos, ClusterTopology topology)
            throws HyracksException {
        scheduler = new org.apache.hyracks.hdfs.scheduler.Scheduler(ncNameToNcInfos, topology);
    }

    /**
     * The constructor of the scheduler.
     *
     * @param ncNameToNcInfos
     *            the mapping from nc names to nc infos
     * @throws HyracksException
     */
    public Scheduler(Map<String, NodeControllerInfo> ncNameToNcInfos, INcCollectionBuilder builder)
            throws HyracksException {
        scheduler = new org.apache.hyracks.hdfs.scheduler.Scheduler(ncNameToNcInfos, builder);
    }

    /**
     * Set location constraints for a file scan operator with a list of file splits
     *
     * @throws HyracksDataException
     */
    public String[] getLocationConstraints(List<InputSplit> splits) throws HyracksException {
        try {
            org.apache.hadoop.mapred.InputSplit[] inputSplits = new org.apache.hadoop.mapred.InputSplit[splits.size()];
            for (int i = 0; i < inputSplits.length; i++)
                inputSplits[i] = new WrappedFileSplit(splits.get(i).getLocations(), splits.get(i).getLength());
            return scheduler.getLocationConstraints(inputSplits);
        } catch (Exception e) {
            throw HyracksException.create(e);
        }
    }
}
