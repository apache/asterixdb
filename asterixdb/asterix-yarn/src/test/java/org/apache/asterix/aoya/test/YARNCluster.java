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

package org.apache.asterix.aoya.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;

/**
 * Manages a Mini (local VM) YARN cluster with a configured number of NodeManager(s).
 */
public class YARNCluster {

    private static final String PATH_TO_HADOOP_CONF = "src/test/resources/hadoop/conf";
    private static final YARNCluster INSTANCE = new YARNCluster();

    private MiniYARNCluster miniCluster;
    private int numDataNodes = 2;
    private Configuration conf = new YarnConfiguration();

    public static YARNCluster getInstance() {
        return INSTANCE;
    }

    private YARNCluster() {

    }

    /**
     * Instantiates the (Mini) DFS Cluster with the configured number of datanodes.
     * Post instantiation, data is laoded to HDFS.
     * Called prior to running the Runtime test suite.
     */
    public void setup() throws Exception {
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/core-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/mapred-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/hdfs-site.xml"));
        conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
        conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, "target/integrationts/data");
        cleanupLocal();
        //this constructor is deprecated in hadoop 2x
        //dfsCluster = new MiniDFSCluster(nameNodePort, conf, numDataNodes, true, true, StartupOption.REGULAR, null);
        miniCluster = new MiniYARNCluster("Asterix_testing", numDataNodes, 1, 1);
        miniCluster.init(conf);
    }

    public MiniYARNCluster getCluster() {
        return miniCluster;
    }

    private void cleanupLocal() throws IOException {
        // cleanup artifacts created on the local file system
        FileSystem lfs = FileSystem.getLocal(new Configuration());
        lfs.delete(new Path("build"), true);
        System.setProperty("hadoop.log.dir", "logs");
    }

    public void cleanup() throws Exception {
        if (miniCluster != null) {
            miniCluster.close();
            cleanupLocal();
        }
    }

}
