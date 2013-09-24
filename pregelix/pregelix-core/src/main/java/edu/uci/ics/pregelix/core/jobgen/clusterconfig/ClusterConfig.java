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
package edu.uci.ics.pregelix.core.jobgen.clusterconfig;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.mapreduce.InputSplit;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.hdfs2.scheduler.Scheduler;

public class ClusterConfig {

    private static String[] NCs;
    private static String storePropertiesPath = "conf/stores.properties";
    private static String clusterPropertiesPath = "conf/cluster.properties";
    private static Properties clusterProperties = new Properties();
    private static Map<String, List<String>> ipToNcMapping;
    private static String[] stores;
    private static Scheduler hdfsScheduler;
    private static Set<String> blackListNodes = new HashSet<String>();
    private static IHyracksClientConnection hcc;

    /**
     * let tests set config path to be whatever
     * 
     * @param propertiesPath
     *            stores properties file path
     */
    public static void setStorePath(String storePropertiesPath) throws HyracksException {
        ClusterConfig.storePropertiesPath = storePropertiesPath;
    }

    public static void setClusterPropertiesPath(String clusterPropertiesPath) throws HyracksException {
        ClusterConfig.clusterPropertiesPath = clusterPropertiesPath;
    }

    /**
     * get NC names running on one IP address
     * 
     * @param ipAddress
     * @return
     * @throws HyracksDataException
     */
    public static List<String> getNCNames(String ipAddress) throws HyracksException {
        return ipToNcMapping.get(ipAddress);
    }

    /**
     * get file split provider
     * 
     * @param jobId
     * @return
     * @throws HyracksDataException
     */
    public static IFileSplitProvider getFileSplitProvider(String jobId, String indexName) throws HyracksException {
        FileSplit[] fileSplits = new FileSplit[stores.length * NCs.length];
        int i = 0;
        for (String nc : NCs) {
            for (String st : stores) {
                FileSplit split = new FileSplit(nc, st + File.separator + nc + "-data" + File.separator + jobId
                        + File.separator + indexName);
                fileSplits[i++] = split;
            }
        }
        return new ConstantFileSplitProvider(fileSplits);
    }

    private static void loadStores() throws HyracksException {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(storePropertiesPath));
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        String store = properties.getProperty("store");
        stores = store.split(",");
    }

    private static void loadClusterProperties() throws HyracksException {
        try {
            clusterProperties.load(new FileInputStream(clusterPropertiesPath));
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public static int getFrameSize() {
        return Integer.parseInt(clusterProperties.getProperty("FRAME_SIZE"));
    }

    /**
     * set location constraint
     * 
     * @param spec
     * @param operator
     * @throws HyracksDataException
     */
    public static void setLocationConstraint(JobSpecification spec, IOperatorDescriptor operator,
            List<InputSplit> splits) throws HyracksException {
        int count = splits.size();
        String[] locations = new String[splits.size()];
        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < splits.size(); i++) {
            try {
                String[] loc = splits.get(i).getLocations();
                Collections.shuffle(Arrays.asList(loc), random);
                if (loc.length > 0) {
                    InetAddress[] allIps = InetAddress.getAllByName(loc[0]);
                    for (InetAddress ip : allIps) {
                        if (ipToNcMapping.get(ip.getHostAddress()) != null) {
                            List<String> ncs = ipToNcMapping.get(ip.getHostAddress());
                            int pos = random.nextInt(ncs.size());
                            locations[i] = ncs.get(pos);
                        } else {
                            int pos = random.nextInt(NCs.length);
                            locations[i] = NCs[pos];
                        }
                    }
                } else {
                    int pos = random.nextInt(NCs.length);
                    locations[i] = NCs[pos];
                }
            } catch (IOException e) {
                throw new HyracksException(e);
            } catch (InterruptedException e) {
                throw new HyracksException(e);
            }
        }
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, operator, locations);
        PartitionConstraintHelper.addPartitionCountConstraint(spec, operator, count);
    }

    /**
     * set location constraint
     * 
     * @param spec
     * @param operator
     * @throws HyracksDataException
     */
    public static void setLocationConstraint(JobSpecification spec, IOperatorDescriptor operator)
            throws HyracksException {
        int count = 0;
        String[] locations = new String[NCs.length * stores.length];
        for (String nc : NCs) {
            for (int i = 0; i < stores.length; i++) {
                locations[count] = nc;
                count++;
            }
        }
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, operator, locations);
    }

    /**
     * set location constraint
     * 
     * @param spec
     * @param operator
     * @throws HyracksDataException
     */
    public static void setCountConstraint(JobSpecification spec, IOperatorDescriptor operator) throws HyracksException {
        int count = NCs.length * stores.length;
        PartitionConstraintHelper.addPartitionCountConstraint(spec, operator, count);
    }

    public static void loadClusterConfig(String ipAddress, int port) throws HyracksException {
        try {
            if (hcc == null) {
                hcc = new HyracksConnection(ipAddress, port);
            }
            Map<String, NodeControllerInfo> ncNameToNcInfos = new TreeMap<String, NodeControllerInfo>();
            ncNameToNcInfos.putAll(hcc.getNodeControllerInfos());

            /**
             * remove black list nodes -- which had disk failures
             */
            for (String blackListNode : blackListNodes) {
                ncNameToNcInfos.remove(blackListNode);
            }

            NCs = new String[ncNameToNcInfos.size()];
            ipToNcMapping = new HashMap<String, List<String>>();
            int i = 0;
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

            hdfsScheduler = new Scheduler(hcc.getNodeControllerInfos(), hcc.getClusterTopology());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

        loadClusterProperties();
        loadStores();
    }

    public static Scheduler getHdfsScheduler() {
        return hdfsScheduler;
    }

    public static String[] getLocationConstraint() throws HyracksException {
        int count = 0;
        String[] locations = new String[NCs.length * stores.length];
        for (String nc : NCs) {
            for (int i = 0; i < stores.length; i++) {
                locations[count] = nc;
                count++;
            }
        }
        return locations;
    }

    public static void addToBlackListNodes(Collection<String> nodes) {
        blackListNodes.addAll(nodes);
    }
}
