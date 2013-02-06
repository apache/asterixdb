/*
 * Copyright 2009-2012 by The Regents of the University of California
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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.mapreduce.InputSplit;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;

/**
 * The scheduler conduct data-local scheduling for data on HDFS
 */
public class Scheduler {

    /** a list of NCs */
    private String[] NCs;

    /** a map from ip to NCs */
    private Map<String, List<String>> ipToNcMapping = new HashMap<String, List<String>>();

    /** a map from the NC name to the index */
    private Map<String, Integer> ncNameToIndex = new HashMap<String, Integer>();

    /**
     * The constructor of the scheduler
     * 
     * @param ncNameToNcInfos
     * @throws HyracksException
     */
    public Scheduler(String ipAddress, int port) throws HyracksException {
        try {
            IHyracksClientConnection hcc = new HyracksConnection(ipAddress, port);
            Map<String, NodeControllerInfo> ncNameToNcInfos = hcc.getNodeControllerInfos();
            loadIPAddressToNCMap(ncNameToNcInfos);
        } catch (Exception e) {
            throw new HyracksException(e);
        }
    }

    public Scheduler(Map<String, NodeControllerInfo> ncNameToNcInfos) throws HyracksException {
        loadIPAddressToNCMap(ncNameToNcInfos);
    }

    /**
     * Set location constraints for a file scan operator with a list of file splits
     * 
     * @throws HyracksDataException
     */
    public String[] getLocationConstraints(List<InputSplit> splits) throws HyracksException {
        int[] capacity = new int[NCs.length];
        Arrays.fill(capacity, 0);
        String[] locations = new String[splits.size()];
        int slots = splits.size() % capacity.length == 0 ? (splits.size() / capacity.length) : (splits.size()
                / capacity.length + 1);

        try {
            Random random = new Random(System.currentTimeMillis());
            boolean scheduled[] = new boolean[splits.size()];
            Arrays.fill(scheduled, false);

            for (int i = 0; i < splits.size(); i++) {
                /**
                 * get the location of all the splits
                 */
                String[] loc = splits.get(i).getLocations();
                if (loc.length > 0) {
                    for (int j = 0; j < loc.length; j++) {
                        /**
                         * get all the IP addresses from the name
                         */
                        InetAddress[] allIps = InetAddress.getAllByName(loc[j]);
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
                                if (capacity[pos] < slots) {
                                    locations[i] = nc;
                                    capacity[pos]++;
                                    scheduled[i] = true;
                                }
                            }
                        }

                        /**
                         * break the loop for data-locations if the schedule has already been found
                         */
                        if (scheduled[i] == true) {
                            break;
                        }
                    }
                }
            }

            /**
             * find the lowest index the current available NCs
             */
            int currentAvailableNC = 0;
            for (int i = 0; i < capacity.length; i++) {
                if (capacity[i] < slots) {
                    currentAvailableNC = i;
                    break;
                }
            }

            /**
             * schedule no-local file reads
             */
            for (int i = 0; i < splits.size(); i++) {
                // if there is no data-local NC choice, choose a random one
                if (!scheduled[i]) {
                    locations[i] = NCs[currentAvailableNC];
                    capacity[currentAvailableNC]++;
                    scheduled[i] = true;

                    /**
                     * move the available NC cursor to the next one
                     */
                    for (int j = currentAvailableNC; j < capacity.length; j++) {
                        if (capacity[j] < slots) {
                            currentAvailableNC = j;
                            break;
                        }
                    }
                }
            }
            return locations;
        } catch (Exception e) {
            throw new HyracksException(e);
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
            int i = 0;

            /**
             * build the IP address to NC map
             */
            for (Map.Entry<String, NodeControllerInfo> entry : ncNameToNcInfos.entrySet()) {
                String ipAddr = InetAddress.getByAddress(entry.getValue().getNetworkAddress().getIpAddress())
                        .getHostAddress();
                List<String> matchedNCs = ipToNcMapping.get(ipAddr);
                if (matchedNCs == null) {
                    matchedNCs = new ArrayList<String>();
                    ipToNcMapping.put(ipAddr, matchedNCs);
                }
                matchedNCs.add(entry.getKey());
                NCs[i] = entry.getKey();
                i++;
            }

            /**
             * set up the NC name to index mapping
             */
            for (i = 0; i < NCs.length; i++) {
                ncNameToIndex.put(NCs[i], i);
            }
        } catch (Exception e) {
            throw new HyracksException(e);
        }
    }
}
