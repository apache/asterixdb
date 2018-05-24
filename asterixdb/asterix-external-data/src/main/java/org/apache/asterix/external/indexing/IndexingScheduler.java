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
package org.apache.asterix.external.indexing;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IndexingScheduler {
    private static final Logger LOGGER = LogManager.getLogger();

    /** a list of NCs */
    private String[] NCs;

    /** a map from ip to NCs */
    private Map<String, List<String>> ipToNcMapping = new HashMap<String, List<String>>();

    /** a map from the NC name to the index */
    private Map<String, Integer> ncNameToIndex = new HashMap<String, Integer>();

    /**
     * The constructor of the scheduler.
     *
     * @param ncNameToNcInfos
     * @throws HyracksException
     */
    public IndexingScheduler(Map<String, NodeControllerInfo> ncNameToNcInfos) throws HyracksException {
        try {
            loadIPAddressToNCMap(ncNameToNcInfos);
        } catch (Exception e) {
            throw HyracksException.create(e);
        }
    }

    /**
     * Set location constraints for a file scan operator with a list of file
     * splits. It tries to assign splits to their local machines fairly
     * Locality is more important than fairness
     *
     * @throws HyracksDataException
     */
    public String[] getLocationConstraints(InputSplit[] splits) throws HyracksException {
        if (splits == null) {
            /* deal the case when the splits array is null */
            return new String[] {};
        }
        int[] workloads = new int[NCs.length];
        Arrays.fill(workloads, 0);
        String[] locations = new String[splits.length];
        Map<String, IntWritable> locationToNumOfSplits = new HashMap<String, IntWritable>();
        /*
         * upper bound is number of splits
         */
        int upperBoundSlots = splits.length;

        try {
            Random random = new Random(System.currentTimeMillis());
            boolean scheduled[] = new boolean[splits.length];
            Arrays.fill(scheduled, false);
            /*
             * scan the splits and build the popularity map
             * give the machines with less local splits more scheduling priority
             */
            buildPopularityMap(splits, locationToNumOfSplits);
            HashMap<String, Integer> locationToNumOfAssignement = new HashMap<String, Integer>();
            for (String location : locationToNumOfSplits.keySet()) {
                locationToNumOfAssignement.put(location, 0);
            }
            /*
             * push data-local upper-bounds slots to each machine
             */
            scheduleLocalSlots(splits, workloads, locations, upperBoundSlots, random, scheduled, locationToNumOfSplits,
                    locationToNumOfAssignement);

            int dataLocalCount = 0;
            for (int i = 0; i < scheduled.length; i++) {
                if (scheduled[i] == true) {
                    dataLocalCount++;
                }
            }
            LOGGER.info("Data local rate: "
                    + (scheduled.length == 0 ? 0.0 : ((float) dataLocalCount / (float) (scheduled.length))));
            /*
             * push non-data-local upper-bounds slots to each machine
             */
            locationToNumOfAssignement.clear();
            for (String nc : NCs) {
                locationToNumOfAssignement.put(nc, 0);
            }
            for (int i = 0; i < scheduled.length; i++) {
                if (scheduled[i]) {
                    locationToNumOfAssignement.put(locations[i], locationToNumOfAssignement.get(locations[i]) + 1);
                }
            }

            scheduleNonLocalSlots(splits, workloads, locations, upperBoundSlots, scheduled, locationToNumOfAssignement);
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
     * @param locationToNumOfAssignement
     */
    private void scheduleNonLocalSlots(InputSplit[] splits, final int[] workloads, String[] locations, int slotLimit,
            boolean[] scheduled, final HashMap<String, Integer> locationToNumOfAssignement)
            throws IOException, UnknownHostException {

        PriorityQueue<String> scheduleCadndiates =
                new PriorityQueue<String>(NCs.length, Comparator.comparing(locationToNumOfAssignement::get));

        scheduleCadndiates.addAll(Arrays.asList(NCs));
        /*
         * schedule no-local file reads
         */
        for (int i = 0; i < splits.length; i++) {
            /* if there is no data-local NC choice, choose a random one */
            if (!scheduled[i]) {
                String selectedNcName = scheduleCadndiates.remove();
                if (selectedNcName != null) {
                    int ncIndex = ncNameToIndex.get(selectedNcName);
                    workloads[ncIndex]++;
                    scheduled[i] = true;
                    locations[i] = selectedNcName;
                    locationToNumOfAssignement.put(selectedNcName, workloads[ncIndex]);
                    scheduleCadndiates.add(selectedNcName);
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
            boolean[] scheduled, final Map<String, IntWritable> locationToNumSplits,
            final HashMap<String, Integer> locationToNumOfAssignement) throws IOException, UnknownHostException {
        /* scheduling candidates will be ordered inversely according to their popularity */
        PriorityQueue<String> scheduleCadndiates = new PriorityQueue<>(3, (s1, s2) -> {
            int assignmentDifference = locationToNumOfAssignement.get(s1).compareTo(locationToNumOfAssignement.get(s2));
            if (assignmentDifference != 0) {
                return assignmentDifference;
            }
            return locationToNumSplits.get(s1).compareTo(locationToNumSplits.get(s2));
        });

        for (int i = 0; i < splits.length; i++) {
            if (scheduled[i]) {
                continue;
            }
            /*
             * get the location of all the splits
             */
            String[] locs = splits[i].getLocations();
            if (locs.length > 0) {
                scheduleCadndiates.clear();
                Collections.addAll(scheduleCadndiates, locs);

                for (String candidate : scheduleCadndiates) {
                    /*
                     * get all the IP addresses from the name
                     */
                    InetAddress[] allIps = InetAddress.getAllByName(candidate);
                    /*
                     * iterate overa all ips
                     */
                    for (InetAddress ip : allIps) {
                        /*
                         * if the node controller exists
                         */
                        if (ipToNcMapping.get(ip.getHostAddress()) != null) {
                            /*
                             * set the ncs
                             */
                            List<String> dataLocations = ipToNcMapping.get(ip.getHostAddress());
                            int arrayPos = random.nextInt(dataLocations.size());
                            String nc = dataLocations.get(arrayPos);
                            int pos = ncNameToIndex.get(nc);
                            /*
                             * check if the node is already full
                             */
                            if (workloads[pos] < slots) {
                                locations[i] = nc;
                                workloads[pos]++;
                                scheduled[i] = true;
                                locationToNumOfAssignement.put(candidate,
                                        locationToNumOfAssignement.get(candidate) + 1);
                                break;
                            }
                        }
                    }
                    /*
                     * break the loop for data-locations if the schedule has
                     * already been found
                     */
                    if (scheduled[i]) {
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
