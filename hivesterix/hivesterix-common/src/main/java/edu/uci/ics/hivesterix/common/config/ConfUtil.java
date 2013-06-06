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
package edu.uci.ics.hivesterix.common.config;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.topology.ClusterTopology;

@SuppressWarnings({ "rawtypes", "deprecation" })
public class ConfUtil {
    private static final String clusterPropertiesPath = "conf/cluster.properties";
    private static final String masterFilePath = "conf/master";

    private static JobConf job;
    private static HiveConf hconf;
    private static String[] NCs;
    private static Map<String, List<String>> ncMapping;
    private static IHyracksClientConnection hcc = null;
    private static ClusterTopology topology = null;
    private static Properties clusterProps;
    private static Map<String, NodeControllerInfo> ncNameToNcInfos;

    public static JobConf getJobConf(Class<? extends InputFormat> format, Path path) {
        JobConf conf = new JobConf();
        if (job != null)
            conf = job;

        String hadoopPath = System.getProperty("HADOOP_HOME", "/hadoop");
        Path pathCore = new Path(hadoopPath + "/conf/core-site.xml");
        conf.addResource(pathCore);
        Path pathMapRed = new Path(hadoopPath + "/conf/mapred-site.xml");
        conf.addResource(pathMapRed);
        Path pathHDFS = new Path(hadoopPath + "/conf/hdfs-site.xml");
        conf.addResource(pathHDFS);

        conf.setInputFormat(format);
        FileInputFormat.setInputPaths(conf, path);
        return conf;
    }

    public static JobConf getJobConf() {
        JobConf conf = new JobConf();
        if (job != null)
            conf = job;

        String hadoopPath = System.getProperty("HADOOP_HOME", "/hadoop");
        Path pathCore = new Path(hadoopPath + "/conf/core-site.xml");
        conf.addResource(pathCore);
        Path pathMapRed = new Path(hadoopPath + "/conf/mapred-site.xml");
        conf.addResource(pathMapRed);
        Path pathHDFS = new Path(hadoopPath + "/conf/hdfs-site.xml");
        conf.addResource(pathHDFS);

        return conf;
    }

    public static void setJobConf(JobConf conf) {
        job = conf;
    }

    public static void setHiveConf(HiveConf hiveConf) {
        hconf = hiveConf;
    }

    public static HiveConf getHiveConf() {
        if (hconf == null) {
            hconf = new HiveConf(SessionState.class);
            hconf.addResource(new Path("conf/hive-default.xml"));
        }
        return hconf;
    }

    public static String[] getNCs() throws HyracksException {
        if (NCs == null) {
            try {
                loadClusterConfig();
            } catch (Exception e) {
                throw new HyracksException(e);
            }
        }
        return NCs;
    }

    public static Map<String, List<String>> getNCMapping() throws HyracksException {
        if (ncMapping == null) {
            try {
                loadClusterConfig();
            } catch (Exception e) {
                throw new HyracksException(e);
            }
        }
        return ncMapping;
    }

    public static Map<String, NodeControllerInfo> getNodeControllerInfo() throws HyracksException {
        if (ncNameToNcInfos == null) {
            try {
                loadClusterConfig();
            } catch (Exception e) {
                throw new HyracksException(e);
            }
        }
        return ncNameToNcInfos;
    }

    private static void loadClusterConfig() {
        try {
            getHiveConf();

            /**
             * load the properties file if it is not loaded
             */
            if (clusterProps == null) {
                clusterProps = new Properties();
                InputStream confIn = new FileInputStream(clusterPropertiesPath);
                clusterProps.load(confIn);
                confIn.close();
            }

            if (hcc == null) {
                BufferedReader ipReader = new BufferedReader(new InputStreamReader(new FileInputStream(masterFilePath)));
                String masterNode = ipReader.readLine();
                ipReader.close();

                InetAddress[] ips = InetAddress.getAllByName(masterNode);
                int port = Integer.parseInt(clusterProps.getProperty("CC_CLIENTPORT"));
                for (InetAddress ip : ips) {
                    if (ip.getAddress().length <= 4) {
                        try {
                            hcc = new HyracksConnection(ip.getHostAddress(), port);
                            break;
                        } catch (Exception e) {
                            continue;
                        }
                    }
                }
            }

            int mpl = Integer.parseInt(hconf.get("hive.hyracks.parrallelism"));
            topology = hcc.getClusterTopology();
            ncNameToNcInfos = hcc.getNodeControllerInfos();
            NCs = new String[ncNameToNcInfos.size() * mpl];
            ncMapping = new HashMap<String, List<String>>();
            int i = 0;
            for (Map.Entry<String, NodeControllerInfo> entry : ncNameToNcInfos.entrySet()) {
                String ipAddr = InetAddress.getByAddress(entry.getValue().getNetworkAddress().getIpAddress())
                        .getHostAddress();
                List<String> matchedNCs = ncMapping.get(ipAddr);
                if (matchedNCs == null) {
                    matchedNCs = new ArrayList<String>();
                    ncMapping.put(ipAddr, matchedNCs);
                }
                matchedNCs.add(entry.getKey());
                for (int j = i * mpl; j < i * mpl + mpl; j++)
                    NCs[j] = entry.getKey();
                i++;
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static ClusterTopology getClusterTopology() {
        if (topology == null)
            loadClusterConfig();
        return topology;
    }
}
