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
package org.apache.hyracks.hdfs.scheduler;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.topology.ClusterTopology;
import org.apache.hyracks.hdfs.api.INcCollection;
import org.apache.hyracks.hdfs.api.INcCollectionBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@SuppressWarnings("deprecation")
public class RackAwareNcCollectionBuilder implements INcCollectionBuilder {
    private static final Logger LOGGER = LogManager.getLogger();
    private ClusterTopology topology;

    public RackAwareNcCollectionBuilder(ClusterTopology topology) {
        this.topology = topology;
    }

    @Override
    public INcCollection build(Map<String, NodeControllerInfo> ncNameToNcInfos,
            final Map<String, List<String>> ipToNcMapping, final Map<String, Integer> ncNameToIndex, String[] NCs,
            final int[] workloads, final int slotLimit) {
        try {
            final Map<List<Integer>, List<String>> pathToNCs = new HashMap<List<Integer>, List<String>>();
            for (String NC : NCs) {
                List<Integer> path = new ArrayList<>();
                String ipAddress = InetAddress
                        .getByAddress(ncNameToNcInfos.get(NC).getNetworkAddress().lookupIpAddress()).getHostAddress();
                topology.lookupNetworkTerminal(ipAddress, path);
                if (path.isEmpty()) {
                    // if the hyracks nc is not in the defined cluster
                    path.add(Integer.MIN_VALUE);
                    LOGGER.info(NC + "'s IP address is not in the cluster toplogy file!");
                }
                List<String> ncs = pathToNCs.computeIfAbsent(path, k -> new ArrayList<>());
                ncs.add(NC);
            }

            final TreeMap<List<Integer>, IntWritable> availableIpsToSlots =
                    new TreeMap<List<Integer>, IntWritable>((l1, l2) -> {
                        int commonLength = Math.min(l1.size(), l2.size());
                        for (int i = 0; i < commonLength; i++) {
                            int value1 = l1.get(i);
                            int value2 = l2.get(i);
                            int cmp = Integer.compare(value1, value2);
                            if (cmp != 0) {
                                return cmp;
                            }
                        }
                        return Integer.compare(l1.size(), l2.size());
                    });
            for (int i = 0; i < workloads.length; i++) {
                if (workloads[i] < slotLimit) {
                    List<Integer> path = new ArrayList<Integer>();
                    String ipAddress =
                            InetAddress.getByAddress(ncNameToNcInfos.get(NCs[i]).getNetworkAddress().lookupIpAddress())
                                    .getHostAddress();
                    topology.lookupNetworkTerminal(ipAddress, path);
                    if (path.isEmpty()) {
                        // if the hyracks nc is not in the defined cluster
                        path.add(Integer.MIN_VALUE);
                    }
                    IntWritable availableSlot = availableIpsToSlots.get(path);
                    if (availableSlot == null) {
                        availableSlot = new IntWritable(slotLimit - workloads[i]);
                        availableIpsToSlots.put(path, availableSlot);
                    } else {
                        availableSlot.set(slotLimit - workloads[i] + availableSlot.get());
                    }
                }
            }
            return new INcCollection() {

                @Override
                public String findNearestAvailableSlot(InputSplit split) {
                    try {
                        String[] locs = split.getLocations();
                        int minDistance = Integer.MAX_VALUE;
                        List<Integer> currentCandidatePath = null;
                        if (locs == null || locs.length > 0) {
                            for (String loc : locs) {
                                /*
                                 * get all the IP addresses from the name
                                 */
                                InetAddress[] allIps = InetAddress.getAllByName(loc);
                                boolean inTopology = false;
                                for (InetAddress ip : allIps) {
                                    List<Integer> splitPath = new ArrayList<>();
                                    boolean inCluster = topology.lookupNetworkTerminal(ip.getHostAddress(), splitPath);
                                    if (!inCluster) {
                                        continue;
                                    }
                                    inTopology = true;
                                    /*
                                     * if the node controller exists
                                     */
                                    List<Integer> candidatePath = availableIpsToSlots.floorKey(splitPath);
                                    if (candidatePath == null) {
                                        candidatePath = availableIpsToSlots.ceilingKey(splitPath);
                                    }
                                    if (candidatePath != null && availableIpsToSlots.get(candidatePath).get() > 0) {
                                        int distance = distance(splitPath, candidatePath);
                                        if (minDistance > distance) {
                                            minDistance = distance;
                                            currentCandidatePath = candidatePath;
                                        }
                                    }
                                }

                                if (!inTopology) {
                                    LOGGER.info(loc + "'s IP address is not in the cluster toplogy file!");
                                    /*
                                     * if the machine is not in the toplogy file
                                     */
                                    List<Integer> candidatePath = null;
                                    for (Entry<List<Integer>, IntWritable> entry : availableIpsToSlots.entrySet()) {
                                        if (entry.getValue().get() > 0) {
                                            candidatePath = entry.getKey();
                                            break;
                                        }
                                    }
                                    /* the split path is empty */
                                    if (candidatePath != null && availableIpsToSlots.get(candidatePath).get() > 0) {
                                        currentCandidatePath = candidatePath;
                                    }
                                }
                            }
                        } else {
                            for (Entry<List<Integer>, IntWritable> entry : availableIpsToSlots.entrySet()) {
                                if (entry.getValue().get() > 0) {
                                    currentCandidatePath = entry.getKey();
                                    break;
                                }
                            }
                        }

                        if (currentCandidatePath != null && !currentCandidatePath.isEmpty()) {
                            /*
                             * Update the entry of the selected IP
                             */
                            IntWritable availableSlot = availableIpsToSlots.get(currentCandidatePath);
                            availableSlot.set(availableSlot.get() - 1);
                            if (availableSlot.get() == 0) {
                                availableIpsToSlots.remove(currentCandidatePath);
                            }
                            /*
                             * Update the entry of the selected NC
                             */
                            List<String> candidateNcs = pathToNCs.get(currentCandidatePath);
                            for (String candidate : candidateNcs) {
                                int ncIndex = ncNameToIndex.get(candidate);
                                if (workloads[ncIndex] < slotLimit) {
                                    return candidate;
                                }
                            }
                        }
                        /* not scheduled */
                        return null;
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                }

                @Override
                public int numAvailableSlots() {
                    return availableIpsToSlots.size();
                }

                private int distance(List<Integer> splitPath, List<Integer> candidatePath) {
                    int commonLength = Math.min(splitPath.size(), candidatePath.size());
                    int distance = 0;
                    for (int i = 0; i < commonLength; i++) {
                        distance = distance * 100 + Math.abs(splitPath.get(i) - candidatePath.get(i));
                    }
                    List<Integer> restElements = splitPath.size() > candidatePath.size() ? splitPath : candidatePath;
                    for (int i = commonLength; i < restElements.size(); i++) {
                        distance = distance * 100 + Math.abs(restElements.get(i));
                    }
                    return distance;
                }
            };
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

}
