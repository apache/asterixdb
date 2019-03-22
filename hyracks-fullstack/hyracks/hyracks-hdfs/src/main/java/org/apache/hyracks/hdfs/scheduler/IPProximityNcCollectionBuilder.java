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
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.hdfs.api.INcCollection;
import org.apache.hyracks.hdfs.api.INcCollectionBuilder;

@SuppressWarnings("deprecation")
public class IPProximityNcCollectionBuilder implements INcCollectionBuilder {

    @Override
    public INcCollection build(Map<String, NodeControllerInfo> ncNameToNcInfos,
            final Map<String, List<String>> ipToNcMapping, final Map<String, Integer> ncNameToIndex, String[] NCs,
            final int[] workloads, final int slotLimit) {
        final TreeMap<BytesWritable, IntWritable> availableIpsToSlots = new TreeMap<BytesWritable, IntWritable>();
        for (int i = 0; i < workloads.length; i++) {
            if (workloads[i] < slotLimit) {
                byte[] rawip;
                try {
                    rawip = ncNameToNcInfos.get(NCs[i]).getNetworkAddress().lookupIpAddress();
                } catch (UnknownHostException e) {
                    // QQQ Should probably have a neater solution than this
                    throw new RuntimeException(e);
                }
                BytesWritable ip = new BytesWritable(rawip);
                IntWritable availableSlot = availableIpsToSlots.get(ip);
                if (availableSlot == null) {
                    availableSlot = new IntWritable(slotLimit - workloads[i]);
                    availableIpsToSlots.put(ip, availableSlot);
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
                    BytesWritable currentCandidateIp = null;
                    if (locs == null || locs.length > 0) {
                        for (int j = 0; j < locs.length; j++) {
                            /**
                             * get all the IP addresses from the name
                             */
                            InetAddress[] allIps = InetAddress.getAllByName(locs[j]);
                            for (InetAddress ip : allIps) {
                                BytesWritable splitIp = new BytesWritable(ip.getAddress());
                                /**
                                 * if the node controller exists
                                 */
                                BytesWritable candidateNcIp = availableIpsToSlots.floorKey(splitIp);
                                if (candidateNcIp == null) {
                                    candidateNcIp = availableIpsToSlots.ceilingKey(splitIp);
                                }
                                if (candidateNcIp != null) {
                                    if (availableIpsToSlots.get(candidateNcIp).get() > 0) {
                                        byte[] candidateIP = candidateNcIp.getBytes();
                                        byte[] splitIP = splitIp.getBytes();
                                        int candidateInt = candidateIP[0] << 24 | (candidateIP[1] & 0xFF) << 16
                                                | (candidateIP[2] & 0xFF) << 8 | (candidateIP[3] & 0xFF);
                                        int splitInt = splitIP[0] << 24 | (splitIP[1] & 0xFF) << 16
                                                | (splitIP[2] & 0xFF) << 8 | (splitIP[3] & 0xFF);
                                        int distance = Math.abs(candidateInt - splitInt);
                                        if (minDistance > distance) {
                                            minDistance = distance;
                                            currentCandidateIp = candidateNcIp;
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        for (Entry<BytesWritable, IntWritable> entry : availableIpsToSlots.entrySet()) {
                            if (entry.getValue().get() > 0) {
                                currentCandidateIp = entry.getKey();
                                break;
                            }
                        }
                    }

                    if (currentCandidateIp != null) {
                        /**
                         * Update the entry of the selected IP
                         */
                        IntWritable availableSlot = availableIpsToSlots.get(currentCandidateIp);
                        availableSlot.set(availableSlot.get() - 1);
                        if (availableSlot.get() == 0) {
                            availableIpsToSlots.remove(currentCandidateIp);
                        }
                        /**
                         * Update the entry of the selected NC
                         */
                        List<String> dataLocations = ipToNcMapping
                                .get(InetAddress.getByAddress(currentCandidateIp.getBytes()).getHostAddress());
                        for (String nc : dataLocations) {
                            int ncIndex = ncNameToIndex.get(nc);
                            if (workloads[ncIndex] < slotLimit) {
                                return nc;
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

        };
    }
}
