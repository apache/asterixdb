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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.topology.ClusterTopology;
import org.apache.hyracks.hdfs.api.INcCollection;
import org.apache.hyracks.hdfs.api.INcCollectionBuilder;
import org.apache.hyracks.ipc.impl.HyracksConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The scheduler conduct data-local scheduling for data reading on HDFS. This
 * class works for Hadoop old API.
 */
public class Scheduler {
    private static final Logger LOGGER = LogManager.getLogger();

    /** a list of NCs */
    private String[] NCs;

    /** a map from ip to NCs */
    private Map<String, List<String>> ipToNcMapping = new HashMap<String, List<String>>();

    /** a map from the NC name to the index */
    private Map<String, Integer> ncNameToIndex = new HashMap<String, Integer>();

    /** a map from NC name to the NodeControllerInfo */
    private Map<String, NodeControllerInfo> ncNameToNcInfos;

    /**
     * the nc collection builder
     */
    private INcCollectionBuilder ncCollectionBuilder;

    /**
     * The constructor of the scheduler.
     *
     * @param ncNameToNcInfos
     * @throws HyracksException
     */

    public Scheduler(String ipAddress, int port) throws HyracksException {
        try {
            IHyracksClientConnection hcc = new HyracksConnection(ipAddress, port);
            this.ncNameToNcInfos = hcc.getNodeControllerInfos();
            ClusterTopology topology = hcc.getClusterTopology();
            this.ncCollectionBuilder = topology == null ? new IPProximityNcCollectionBuilder()
                    : new RackAwareNcCollectionBuilder(topology);
            loadIPAddressToNCMap(ncNameToNcInfos);
        } catch (Exception e) {
            throw HyracksException.create(e);
        }
    }

    /**
     * The constructor of the scheduler.
     *
     * @param ncNameToNcInfos
     * @throws HyracksException
     */
    public Scheduler(String ipAddress, int port, INcCollectionBuilder ncCollectionBuilder) throws HyracksException {
        try {
            IHyracksClientConnection hcc = new HyracksConnection(ipAddress, port);
            this.ncNameToNcInfos = hcc.getNodeControllerInfos();
            this.ncCollectionBuilder = ncCollectionBuilder;
            loadIPAddressToNCMap(ncNameToNcInfos);
        } catch (Exception e) {
            throw HyracksException.create(e);
        }
    }

    /**
     * The constructor of the scheduler.
     *
     * @param ncNameToNcInfos
     *            the mapping from nc names to nc infos
     * @throws HyracksException
     */
    public Scheduler(Map<String, NodeControllerInfo> ncNameToNcInfos) throws HyracksException {
        this.ncNameToNcInfos = ncNameToNcInfos;
        this.ncCollectionBuilder = new IPProximityNcCollectionBuilder();
        loadIPAddressToNCMap(ncNameToNcInfos);
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
        this(ncNameToNcInfos);
        this.ncCollectionBuilder =
                topology == null ? new IPProximityNcCollectionBuilder() : new RackAwareNcCollectionBuilder(topology);
    }

    /**
     * The constructor of the scheduler.
     *
     * @param ncNameToNcInfos
     *            the mapping from nc names to nc infos
     * @throws HyracksException
     */
    public Scheduler(Map<String, NodeControllerInfo> ncNameToNcInfos, INcCollectionBuilder ncCollectionBuilder)
            throws HyracksException {
        this.ncNameToNcInfos = ncNameToNcInfos;
        this.ncCollectionBuilder = ncCollectionBuilder;
        loadIPAddressToNCMap(ncNameToNcInfos);
    }

    /**
     * Set location constraints for a file scan operator with a list of file
     * splits. It guarantees the maximum slots a machine can is at most one more
     * than the minimum slots a machine can get.
     *
     * @throws HyracksDataException
     */
    public String[] getLocationConstraints(InputSplit[] splits) throws HyracksException {
        if (splits == null) {
            /** deal the case when the splits array is null */
            return new String[] {};
        }
        int[] workloads = new int[NCs.length];
        Arrays.fill(workloads, 0);
        String[] locations = new String[splits.length];
        Map<String, IntWritable> locationToNumOfSplits = new HashMap<String, IntWritable>();
        /**
         * upper bound number of slots that a machine can get
         */
        int upperBoundSlots = splits.length % workloads.length == 0 ? (splits.length / workloads.length)
                : (splits.length / workloads.length + 1);
        /**
         * lower bound number of slots that a machine can get
         */
        int lowerBoundSlots = splits.length % workloads.length == 0 ? upperBoundSlots : upperBoundSlots - 1;

        try {
            Random random = new Random(System.currentTimeMillis());
            boolean scheduled[] = new boolean[splits.length];
            Arrays.fill(scheduled, false);
            /**
             * scan the splits and build the popularity map
             * give the machines with less local splits more scheduling priority
             */
            buildPopularityMap(splits, locationToNumOfSplits);
            /**
             * push data-local lower-bounds slots to each machine
             */
            scheduleLocalSlots(splits, workloads, locations, lowerBoundSlots, random, scheduled, locationToNumOfSplits);
            /**
             * push data-local upper-bounds slots to each machine
             */
            scheduleLocalSlots(splits, workloads, locations, upperBoundSlots, random, scheduled, locationToNumOfSplits);

            int dataLocalCount = 0;
            for (int i = 0; i < scheduled.length; i++) {
                if (scheduled[i] == true) {
                    dataLocalCount++;
                }
            }
            LOGGER.info("Data local rate: "
                    + (scheduled.length == 0 ? 0.0 : ((float) dataLocalCount / (float) (scheduled.length))));
            /**
             * push non-data-local lower-bounds slots to each machine
             */
            scheduleNonLocalSlots(splits, workloads, locations, lowerBoundSlots, scheduled);
            /**
             * push non-data-local upper-bounds slots to each machine
             */
            scheduleNonLocalSlots(splits, workloads, locations, upperBoundSlots, scheduled);
            return locations;
        } catch (IOException e) {
            throw HyracksException.create(e);
        }
    }

    /**
     * Schedule non-local slots to each machine
     *
     * @param splits
     *            The HDFS file splits.
     * @param workloads
     *            The current capacity of each machine.
     * @param locations
     *            The result schedule.
     * @param slotLimit
     *            The maximum slots of each machine.
     * @param scheduled
     *            Indicate which slot is scheduled.
     */
    private void scheduleNonLocalSlots(InputSplit[] splits, int[] workloads, String[] locations, int slotLimit,
            boolean[] scheduled) throws IOException, UnknownHostException {
        /**
         * build the map from available ips to the number of available slots
         */
        INcCollection ncCollection = this.ncCollectionBuilder.build(ncNameToNcInfos, ipToNcMapping, ncNameToIndex, NCs,
                workloads, slotLimit);
        if (ncCollection.numAvailableSlots() == 0) {
            return;
        }
        /**
         * schedule no-local file reads
         */
        for (int i = 0; i < splits.length; i++) {
            /** if there is no data-local NC choice, choose a random one */
            if (!scheduled[i]) {
                InputSplit split = splits[i];
                String selectedNcName = ncCollection.findNearestAvailableSlot(split);
                if (selectedNcName != null) {
                    int ncIndex = ncNameToIndex.get(selectedNcName);
                    workloads[ncIndex]++;
                    scheduled[i] = true;
                    locations[i] = selectedNcName;
                }
            }
        }
    }

    /**
     * Schedule data-local slots to each machine.
     *
     * @param splits
     *            The HDFS file splits.
     * @param workloads
     *            The current capacity of each machine.
     * @param locations
     *            The result schedule.
     * @param slots
     *            The maximum slots of each machine.
     * @param random
     *            The random generator.
     * @param scheduled
     *            Indicate which slot is scheduled.
     * @throws IOException
     * @throws UnknownHostException
     */
    private void scheduleLocalSlots(InputSplit[] splits, int[] workloads, String[] locations, int slots, Random random,
            boolean[] scheduled, final Map<String, IntWritable> locationToNumSplits)
            throws IOException, UnknownHostException {
        /** scheduling candidates will be ordered inversely according to their popularity */
        PriorityQueue<String> scheduleCadndiates = new PriorityQueue<String>(3, new Comparator<String>() {

            @Override
            public int compare(String s1, String s2) {
                return locationToNumSplits.get(s1).compareTo(locationToNumSplits.get(s2));
            }

        });
        for (int i = 0; i < splits.length; i++) {
            if (scheduled[i]) {
                continue;
            }
            /**
             * get the location of all the splits
             */
            String[] locs = splits[i].getLocations();
            if (locs.length > 0) {
                scheduleCadndiates.clear();
                for (int j = 0; j < locs.length; j++) {
                    scheduleCadndiates.add(locs[j]);
                }

                for (String candidate : scheduleCadndiates) {
                    /**
                     * get all the IP addresses from the name
                     */
                    InetAddress[] allIps = InetAddress.getAllByName(candidate);
                    /**
                     * iterate overa all ips
                     */
                    for (InetAddress ip : allIps) {
                        /**
                         * if the node controller exists
                         */
                        if (ipToNcMapping.get(ip.getHostAddress()) != null) {
                            /**
                             * set the ncs
                             */
                            List<String> dataLocations = ipToNcMapping.get(ip.getHostAddress());
                            int arrayPos = random.nextInt(dataLocations.size());
                            String nc = dataLocations.get(arrayPos);
                            int pos = ncNameToIndex.get(nc);
                            /**
                             * check if the node is already full
                             */
                            if (workloads[pos] < slots) {
                                locations[i] = nc;
                                workloads[pos]++;
                                scheduled[i] = true;
                                break;
                            }
                        }
                    }
                    /**
                     * break the loop for data-locations if the schedule has
                     * already been found
                     */
                    if (scheduled[i] == true) {
                        break;
                    }
                }
            }
        }
    }

    /**
     * Scan the splits once and build a popularity map
     *
     * @param splits
     *            the split array
     * @param locationToNumOfSplits
     *            the map to be built
     * @throws IOException
     */
    private void buildPopularityMap(InputSplit[] splits, Map<String, IntWritable> locationToNumOfSplits)
            throws IOException {
        for (InputSplit split : splits) {
            String[] locations = split.getLocations();
            for (String loc : locations) {
                IntWritable locCount = locationToNumOfSplits.get(loc);
                if (locCount == null) {
                    locCount = new IntWritable(0);
                    locationToNumOfSplits.put(loc, locCount);
                }
                locCount.set(locCount.get() + 1);
            }
        }
    }

    /**
     * Load the IP-address-to-NC map from the NCNameToNCInfoMap
     *
     * @param ncNameToNcInfos
     * @throws HyracksException
     */
    private void loadIPAddressToNCMap(Map<String, NodeControllerInfo> ncNameToNcInfos) throws HyracksException {
        try {
            NCs = new String[ncNameToNcInfos.size()];
            ipToNcMapping.clear();
            ncNameToIndex.clear();
            int i = 0;

            /*
             * build the IP address to NC map
             */
            for (Map.Entry<String, NodeControllerInfo> entry : ncNameToNcInfos.entrySet()) {
                String ipAddr = InetAddress.getByAddress(entry.getValue().getNetworkAddress().lookupIpAddress())
                        .getHostAddress();
                List<String> matchedNCs = ipToNcMapping.computeIfAbsent(ipAddr, k -> new ArrayList<>());
                matchedNCs.add(entry.getKey());
                NCs[i] = entry.getKey();
                i++;
            }

            /*
             * set up the NC name to index mapping
             */
            for (i = 0; i < NCs.length; i++) {
                ncNameToIndex.put(NCs[i], i);
            }
        } catch (Exception e) {
            throw HyracksException.create(e);
        }
    }
}
