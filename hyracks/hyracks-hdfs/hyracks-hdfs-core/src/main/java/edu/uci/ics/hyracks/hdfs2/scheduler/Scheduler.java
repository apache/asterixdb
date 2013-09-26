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

package edu.uci.ics.hyracks.hdfs2.scheduler;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.InputSplit;

import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.topology.ClusterTopology;
import edu.uci.ics.hyracks.hdfs.api.INcCollectionBuilder;

/**
 * The scheduler conduct data-local scheduling for data reading on HDFS.
 * This class works for Hadoop new API.
 */
@SuppressWarnings("deprecation")
public class Scheduler {

    private edu.uci.ics.hyracks.hdfs.scheduler.Scheduler scheduler;

    /**
     * The constructor of the scheduler
     * 
     * @param ncNameToNcInfos
     * @throws HyracksException
     */
    public Scheduler(String ipAddress, int port) throws HyracksException {
        scheduler = new edu.uci.ics.hyracks.hdfs.scheduler.Scheduler(ipAddress, port);
    }

    /**
     * The constructor of the scheduler.
     * 
     * @param ncNameToNcInfos
     *            the mapping from nc names to nc infos
     * @throws HyracksException
     */
    public Scheduler(Map<String, NodeControllerInfo> ncNameToNcInfos) throws HyracksException {
        scheduler = new edu.uci.ics.hyracks.hdfs.scheduler.Scheduler(ncNameToNcInfos);
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
    public Scheduler(Map<String, NodeControllerInfo> ncNameToNcInfos, ClusterTopology topology) throws HyracksException {
        scheduler = new edu.uci.ics.hyracks.hdfs.scheduler.Scheduler(ncNameToNcInfos, topology);
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
        scheduler = new edu.uci.ics.hyracks.hdfs.scheduler.Scheduler(ncNameToNcInfos, builder);
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
            throw new HyracksException(e);
        }
    }
}
