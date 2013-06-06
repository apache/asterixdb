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
package edu.uci.ics.hyracks.hdfs.scheduler;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.logging.Logger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.InputSplit;

import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.topology.ClusterTopology;
import edu.uci.ics.hyracks.hdfs.api.INcCollection;
import edu.uci.ics.hyracks.hdfs.api.INcCollectionBuilder;

@SuppressWarnings("deprecation")
public class RackAwareNcCollectionBuilder implements INcCollectionBuilder {
    private static final Logger LOGGER = Logger.getLogger(RackAwareNcCollectionBuilder.class.getName());
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
            for (int i = 0; i < NCs.length; i++) {
                List<Integer> path = new ArrayList<Integer>();
                String ipAddress = InetAddress.getByAddress(
                        ncNameToNcInfos.get(NCs[i]).getNetworkAddress().getIpAddress()).getHostAddress();
                topology.lookupNetworkTerminal(ipAddress, path);
                if (path.size() <= 0) {
                    // if the hyracks nc is not in the defined cluster
                    path.add(Integer.MIN_VALUE);
                    LOGGER.info(NCs[i] + "'s IP address is not in the cluster toplogy file!");
                }
                List<String> ncs = pathToNCs.get(path);
                if (ncs == null) {
                    ncs = new ArrayList<String>();
                    pathToNCs.put(path, ncs);
                }
                ncs.add(NCs[i]);
            }

            final TreeMap<List<Integer>, IntWritable> availableIpsToSlots = new TreeMap<List<Integer>, IntWritable>(
                    new Comparator<List<Integer>>() {

                        @Override
                        public int compare(List<Integer> l1, List<Integer> l2) {
                            int commonLength = Math.min(l1.size(), l2.size());
                            for (int i = 0; i < commonLength; i++) {
                                Integer value1 = l1.get(i);
                                Integer value2 = l2.get(i);
                                int cmp = value1 > value2 ? 1 : (value1 < value2 ? -1 : 0);
                                if (cmp != 0) {
                                    return cmp;
                                }
                            }
                            return l1.size() > l2.size() ? 1 : (l1.size() < l2.size() ? -1 : 0);
                        }

                    });
            for (int i = 0; i < workloads.length; i++) {
                if (workloads[i] < slotLimit) {
                    List<Integer> path = new ArrayList<Integer>();
                    String ipAddress = InetAddress.getByAddress(
                            ncNameToNcInfos.get(NCs[i]).getNetworkAddress().getIpAddress()).getHostAddress();
                    topology.lookupNetworkTerminal(ipAddress, path);
                    if (path.size() <= 0) {
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
                            for (int j = 0; j < locs.length; j++) {
                                /**
                                 * get all the IP addresses from the name
                                 */
                                InetAddress[] allIps = InetAddress.getAllByName(locs[j]);
                                boolean inTopology = false;
                                for (InetAddress ip : allIps) {
                                    List<Integer> splitPath = new ArrayList<Integer>();
                                    boolean inCluster = topology.lookupNetworkTerminal(ip.getHostAddress(), splitPath);
                                    if (!inCluster) {
                                        continue;
                                    }
                                    inTopology = true;
                                    /**
                                     * if the node controller exists
                                     */
                                    List<Integer> candidatePath = availableIpsToSlots.floorKey(splitPath);
                                    if (candidatePath == null) {
                                        candidatePath = availableIpsToSlots.ceilingKey(splitPath);
                                    }
                                    if (candidatePath != null) {
                                        if (availableIpsToSlots.get(candidatePath).get() > 0) {
                                            int distance = distance(splitPath, candidatePath);
                                            if (minDistance > distance) {
                                                minDistance = distance;
                                                currentCandidatePath = candidatePath;
                                            }
                                        }

                                    }
                                }

                                if (!inTopology) {
                                    LOGGER.info(locs[j] + "'s IP address is not in the cluster toplogy file!");
                                    /**
                                     * if the machine is not in the toplogy file
                                     */
                                    List<Integer> candidatePath = null;
                                    for (Entry<List<Integer>, IntWritable> entry : availableIpsToSlots.entrySet()) {
                                        if (entry.getValue().get() > 0) {
                                            candidatePath = entry.getKey();
                                            break;
                                        }
                                    }
                                    /** the split path is empty */
                                    if (candidatePath != null) {
                                        if (availableIpsToSlots.get(candidatePath).get() > 0) {
                                            currentCandidatePath = candidatePath;
                                        }
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

                        if (currentCandidatePath != null && currentCandidatePath.size() > 0) {
                            /**
                             * Update the entry of the selected IP
                             */
                            IntWritable availableSlot = availableIpsToSlots.get(currentCandidatePath);
                            availableSlot.set(availableSlot.get() - 1);
                            if (availableSlot.get() == 0) {
                                availableIpsToSlots.remove(currentCandidatePath);
                            }
                            /**
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
                        /** not scheduled */
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
