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
package edu.uci.ics.pregelix.example.util;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.pregelix.core.jobgen.clusterconfig.ClusterConfig;
import edu.uci.ics.pregelix.core.util.PregelixHyracksIntegrationUtil;

@SuppressWarnings("deprecation")
public class TestCluster {
    private static final Logger LOGGER = Logger.getLogger(TestCluster.class.getName());

    private static final String ACTUAL_RESULT_DIR = "actual";
    private static final String PATH_TO_HADOOP_CONF = "src/test/resources/hadoop/conf";
    private static final String PATH_TO_CLUSTER_STORE = "src/test/resources/cluster/stores.properties";
    private static final String PATH_TO_CLUSTER_PROPERTIES = "src/test/resources/cluster/cluster.properties";

    private static final String DATA_PATH = "data/webmap/webmap_link.txt";
    private static final String HDFS_PATH = "/webmap/";

    private static final String DATA_PATH2 = "data/webmapcomplex/webmap_link.txt";
    private static final String HDFS_PATH2 = "/webmapcomplex/";

    private static final String DATA_PATH3 = "data/clique/clique.txt";
    private static final String HDFS_PATH3 = "/clique/";

    private static final String DATA_PATH4 = "data/clique2/clique.txt";
    private static final String HDFS_PATH4 = "/clique2/";

    private static final String DATA_PATH5 = "data/clique3/clique.txt";
    private static final String HDFS_PATH5 = "/clique3/";

    private static final String HADOOP_CONF_PATH = ACTUAL_RESULT_DIR + File.separator + "conf.xml";
    private MiniDFSCluster dfsCluster;

    private JobConf conf = new JobConf();
    private int numberOfNC = 2;

    public void setUp() throws Exception {
        ClusterConfig.setStorePath(PATH_TO_CLUSTER_STORE);
        ClusterConfig.setClusterPropertiesPath(PATH_TO_CLUSTER_PROPERTIES);
        cleanupStores();
        PregelixHyracksIntegrationUtil.init();
        LOGGER.info("Hyracks mini-cluster started");
        FileUtils.forceMkdir(new File(ACTUAL_RESULT_DIR));
        FileUtils.cleanDirectory(new File(ACTUAL_RESULT_DIR));
        startHDFS();
    }

    private void cleanupStores() throws IOException {
        FileUtils.forceMkdir(new File("teststore"));
        FileUtils.forceMkdir(new File("build"));
        FileUtils.cleanDirectory(new File("teststore"));
        FileUtils.cleanDirectory(new File("build"));
    }

    private void startHDFS() throws IOException {
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/core-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/mapred-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/hdfs-site.xml"));
        FileSystem lfs = FileSystem.getLocal(new Configuration());
        lfs.delete(new Path("build"), true);
        System.setProperty("hadoop.log.dir", "logs");
        dfsCluster = new MiniDFSCluster(conf, numberOfNC, true, null);
        FileSystem dfs = FileSystem.get(conf);
        Path src = new Path(DATA_PATH);
        Path dest = new Path(HDFS_PATH);
        dfs.mkdirs(dest);
        dfs.copyFromLocalFile(src, dest);

        src = new Path(DATA_PATH2);
        dest = new Path(HDFS_PATH2);
        dfs.mkdirs(dest);
        dfs.copyFromLocalFile(src, dest);

        src = new Path(DATA_PATH3);
        dest = new Path(HDFS_PATH3);
        dfs.mkdirs(dest);
        dfs.copyFromLocalFile(src, dest);

        src = new Path(DATA_PATH4);
        dest = new Path(HDFS_PATH4);
        dfs.mkdirs(dest);
        dfs.copyFromLocalFile(src, dest);

        src = new Path(DATA_PATH5);
        dest = new Path(HDFS_PATH5);
        dfs.mkdirs(dest);
        dfs.copyFromLocalFile(src, dest);

        DataOutputStream confOutput = new DataOutputStream(new FileOutputStream(new File(HADOOP_CONF_PATH)));
        conf.writeXml(confOutput);
        confOutput.flush();
        confOutput.close();
    }

    /**
     * cleanup hdfs cluster
     */
    public void cleanupHDFS() throws Exception {
        dfsCluster.shutdown();
    }

    public void tearDown() throws Exception {
        PregelixHyracksIntegrationUtil.deinit();
        LOGGER.info("Hyracks mini-cluster shut down");
        cleanupHDFS();
    }

    protected static List<String> getFileList(String ignorePath) throws FileNotFoundException, IOException {
        BufferedReader reader = new BufferedReader(new FileReader(ignorePath));
        String s = null;
        List<String> ignores = new ArrayList<String>();
        while ((s = reader.readLine()) != null) {
            ignores.add(s);
        }
        reader.close();
        return ignores;
    }

}
