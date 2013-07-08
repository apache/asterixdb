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
package edu.uci.ics.hivesterix.test.base;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import junit.framework.TestSuite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.hivesterix.common.config.ConfUtil;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.common.controllers.CCConfig;
import edu.uci.ics.hyracks.control.common.controllers.NCConfig;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;

@SuppressWarnings("deprecation")
public abstract class AbstractTestSuiteClass extends TestSuite {

    private static final String PATH_TO_HADOOP_CONF = "src/test/resources/runtimefunctionts/hadoop/conf";
    private static final String PATH_TO_HIVE_CONF = "src/test/resources/runtimefunctionts/hive/conf/hive-default.xml";

    private static final String PATH_TO_CLUSTER_CONF = "src/test/resources/runtimefunctionts/hive/conf/topology.xml";
    private static final String PATH_TO_DATA = "src/test/resources/runtimefunctionts/data/";

    private static final String clusterPropertiesPath = "conf/cluster.properties";
    private static final String masterFilePath = "conf/master";

    private Properties clusterProps;
    private MiniDFSCluster dfsCluster;

    private JobConf conf = new JobConf();
    protected FileSystem dfs;

    private int numberOfNC = 2;
    private ClusterControllerService cc;
    private Map<String, NodeControllerService> ncs = new HashMap<String, NodeControllerService>();

    /**
     * setup cluster
     * 
     * @throws IOException
     */
    protected void setup() throws Exception {
        setupHdfs();
        setupHyracks();
    }

    private void setupHdfs() throws IOException {
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/core-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/mapred-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/hdfs-site.xml"));
        HiveConf hconf = new HiveConf(SessionState.class);
        hconf.addResource(new Path(PATH_TO_HIVE_CONF));

        FileSystem lfs = FileSystem.getLocal(new Configuration());
        lfs.delete(new Path("build"), true);
        lfs.delete(new Path("metastore_db"), true);

        System.setProperty("hadoop.log.dir", "logs");
        dfsCluster = new MiniDFSCluster(hconf, numberOfNC, true, null);
        dfs = dfsCluster.getFileSystem();
        conf = new JobConf(hconf);
        ConfUtil.setJobConf(conf);

        String fsName = conf.get("fs.default.name");
        hconf.set("hive.metastore.warehouse.dir", fsName.concat("/tmp/hivesterix"));
        String warehouse = hconf.get("hive.metastore.warehouse.dir");
        dfs.mkdirs(new Path(warehouse));
        ConfUtil.setHiveConf(hconf);
    }

    private void setupHyracks() throws Exception {
        // read hive conf
        HiveConf hconf = new HiveConf(SessionState.class);
        hconf.addResource(new Path(PATH_TO_HIVE_CONF));
        SessionState.start(hconf);
        /**
         * load the properties file if it is not loaded
         */
        if (clusterProps == null) {
            clusterProps = new Properties();
            InputStream confIn = new FileInputStream(clusterPropertiesPath);
            clusterProps.load(confIn);
            confIn.close();
        }
        BufferedReader ipReader = new BufferedReader(new InputStreamReader(new FileInputStream(masterFilePath)));
        String masterNode = ipReader.readLine();
        ipReader.close();
        InetAddress[] ips = InetAddress.getAllByName(masterNode);
        String ipAddress = null;
        for (InetAddress ip : ips) {
            if (ip.getAddress().length <= 4) {
                ipAddress = ip.getHostAddress();
            }
        }
        int clientPort = Integer.parseInt(clusterProps.getProperty("CC_CLIENTPORT"));
        int netPort = Integer.parseInt(clusterProps.getProperty("CC_CLUSTERPORT"));

        // start hyracks cc
        CCConfig ccConfig = new CCConfig();
        ccConfig.clientNetIpAddress = ipAddress;
        ccConfig.clientNetPort = clientPort;
        ccConfig.clusterNetPort = netPort;
        ccConfig.profileDumpPeriod = 1000;
        ccConfig.heartbeatPeriod = 200000000;
        ccConfig.maxHeartbeatLapsePeriods = 200000000;
        ccConfig.clusterTopologyDefinition = new File(PATH_TO_CLUSTER_CONF);
        cc = new ClusterControllerService(ccConfig);
        cc.start();

        // start hyracks nc
        for (int i = 0; i < numberOfNC; i++) {
            NCConfig ncConfig = new NCConfig();
            ncConfig.ccHost = ipAddress;
            ncConfig.clusterNetIPAddress = ipAddress;
            ncConfig.ccPort = netPort;
            ncConfig.dataIPAddress = "127.0.0.1";
            ncConfig.datasetIPAddress = "127.0.0.1";
            ncConfig.nodeId = "nc" + i;
            NodeControllerService nc = new NodeControllerService(ncConfig);
            nc.start();
            ncs.put(ncConfig.nodeId, nc);
        }
    }

    protected void makeDir(String path) throws IOException {
        dfs.mkdirs(new Path(path));
    }

    protected void loadFiles(String src, String dest) throws IOException {
        dfs.copyFromLocalFile(new Path(src), new Path(dest));
    }

    protected void cleanup() throws Exception {
        cleanupHdfs();
        cleanupHyracks();
    }

    /**
     * cleanup hdfs cluster
     */
    private void cleanupHdfs() throws IOException {
        dfs.delete(new Path("/"), true);
        FileSystem.closeAll();
        dfsCluster.shutdown();
    }

    /**
     * cleanup hyracks cluster
     */
    private void cleanupHyracks() throws Exception {
        Iterator<NodeControllerService> iterator = ncs.values().iterator();
        while (iterator.hasNext()) {
            NodeControllerService nc = iterator.next();
            nc.stop();
        }
        cc.stop();
    }

    protected static List<String> getIgnoreList(String ignorePath) throws FileNotFoundException, IOException {
        BufferedReader reader = new BufferedReader(new FileReader(ignorePath));
        String s = null;
        List<String> ignores = new ArrayList<String>();
        while ((s = reader.readLine()) != null) {
            ignores.add(s);
        }
        reader.close();
        return ignores;
    }

    protected static boolean isIgnored(String q, List<String> ignoreList) {
        for (String ignore : ignoreList) {
            if (q.indexOf(ignore) >= 0) {
                return true;
            }
        }
        return false;
    }

    protected void loadData() throws IOException {

        makeDir("/tpch");
        makeDir("/tpch/customer");
        makeDir("/tpch/lineitem");
        makeDir("/tpch/orders");
        makeDir("/tpch/part");
        makeDir("/tpch/partsupp");
        makeDir("/tpch/supplier");
        makeDir("/tpch/nation");
        makeDir("/tpch/region");

        makeDir("/test");
        makeDir("/test/joinsrc1");
        makeDir("/test/joinsrc2");

        loadFiles(PATH_TO_DATA + "customer.tbl", "/tpch/customer/");
        loadFiles(PATH_TO_DATA + "lineitem.tbl", "/tpch/lineitem/");
        loadFiles(PATH_TO_DATA + "orders.tbl", "/tpch/orders/");
        loadFiles(PATH_TO_DATA + "part.tbl", "/tpch/part/");
        loadFiles(PATH_TO_DATA + "partsupp.tbl", "/tpch/partsupp/");
        loadFiles(PATH_TO_DATA + "supplier.tbl", "/tpch/supplier/");
        loadFiles(PATH_TO_DATA + "nation.tbl", "/tpch/nation/");
        loadFiles(PATH_TO_DATA + "region.tbl", "/tpch/region/");

        loadFiles(PATH_TO_DATA + "large_card_join_src.tbl", "/test/joinsrc1/");
        loadFiles(PATH_TO_DATA + "large_card_join_src_small.tbl", "/test/joinsrc2/");
    }

}
