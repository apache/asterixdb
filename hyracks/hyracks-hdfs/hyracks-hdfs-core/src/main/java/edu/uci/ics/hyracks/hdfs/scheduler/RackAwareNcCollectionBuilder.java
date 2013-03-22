package edu.uci.ics.hyracks.hdfs.scheduler;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.InputSplit;

import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.topology.ClusterTopology;
import edu.uci.ics.hyracks.hdfs.api.INcCollection;
import edu.uci.ics.hyracks.hdfs.api.INcCollectionBuilder;

@SuppressWarnings("deprecation")
public class RackAwareNcCollectionBuilder implements INcCollectionBuilder {
    private ClusterTopology topology;

    public RackAwareNcCollectionBuilder(ClusterTopology topology) {
        this.topology = topology;
    }

    @Override
    public INcCollection build(Map<String, NodeControllerInfo> ncNameToNcInfos,
            final Map<String, List<String>> ipToNcMapping, final Map<String, Integer> ncNameToIndex, String[] NCs,
            final int[] workloads, final int slotLimit) {
        try {
            final Map<List<Integer>, String> pathToNCs = new HashMap<List<Integer>, String>();
            for (int i = 0; i < NCs.length; i++) {
                List<Integer> path = new ArrayList<Integer>();
                String ipAddress = InetAddress.getByAddress(
                        ncNameToNcInfos.get(NCs[i]).getNetworkAddress().getIpAddress()).getHostAddress();
                topology.lookupNetworkTerminal(ipAddress, path);
                pathToNCs.put(path, NCs[i]);
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
                                for (InetAddress ip : allIps) {
                                    List<Integer> splitPath = new ArrayList<Integer>();
                                    boolean inCluster = topology.lookupNetworkTerminal(ip.getHostAddress(), splitPath);
                                    if (!inCluster) {
                                        continue;
                                    }
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
                            }
                        } else {
                            for (Entry<List<Integer>, IntWritable> entry : availableIpsToSlots.entrySet()) {
                                if (entry.getValue().get() > 0) {
                                    currentCandidatePath = entry.getKey();
                                    break;
                                }
                            }
                        }

                        if (currentCandidatePath.size() > 0) {
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
                            String candidateNc = pathToNCs.get(currentCandidatePath);
                            int ncIndex = ncNameToIndex.get(candidateNc);
                            if (workloads[ncIndex] < slotLimit) {
                                return candidateNc;
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
                        distance = distance * 10 + Math.abs(splitPath.get(i) - candidatePath.get(i));
                    }
                    return distance;
                }
            };
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

}
