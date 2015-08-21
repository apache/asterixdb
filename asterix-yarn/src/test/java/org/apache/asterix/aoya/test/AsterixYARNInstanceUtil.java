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
package edu.uci.ics.asterix.aoya.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.util.logging.Logger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.Assert;

import edu.uci.ics.asterix.aoya.AsterixYARNClient;
import edu.uci.ics.asterix.aoya.Utils;
import edu.uci.ics.asterix.event.schema.yarnCluster.Cluster;
import edu.uci.ics.asterix.event.schema.yarnCluster.Node;

public class AsterixYARNInstanceUtil {
    private static final String PATH_ACTUAL = "ittest/";
    private static final String INSTANCE_NAME = "asterix-integration-test";
    private MiniYARNCluster miniCluster;
    private YarnConfiguration appConf;
    public String aoyaHome;
    public String configPath;
    public String aoyaServerPath;
    public String parameterPath;

    public YarnConfiguration setUp() throws Exception {
        File asterixProjectDir = new File(System.getProperty("user.dir"));

        File installerTargetDir = new File(asterixProjectDir, "target");

        String[] dirsInTarget = installerTargetDir.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return new File(dir, name).isDirectory() && name.startsWith("asterix-yarn")
                        && name.endsWith("binary-assembly");
            }

        });
        if (dirsInTarget.length != 1) {
            throw new IllegalStateException("Could not find binary to run YARN integration test with");
        }
        aoyaHome = installerTargetDir.getAbsolutePath() + File.separator + dirsInTarget[0];
        File asterixServerInstallerDir = new File(aoyaHome, "asterix");
        String[] zipsInFolder = asterixServerInstallerDir.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith("asterix-server") && name.endsWith("binary-assembly.zip");
            }
        });
        if (zipsInFolder.length != 1) {
            throw new IllegalStateException("Could not find server binary to run YARN integration test with");
        }
        aoyaServerPath = asterixServerInstallerDir.getAbsolutePath() + File.separator + zipsInFolder[0];
        configPath = aoyaHome + File.separator + "configs" + File.separator + "local.xml";
        parameterPath = aoyaHome + File.separator + "conf" + File.separator + "base-asterix-configuration.xml";
        YARNCluster.getInstance().setup();
        appConf = new YarnConfiguration();
        File baseDir = new File("./target/hdfs/").getAbsoluteFile();
        FileUtil.fullyDelete(baseDir);
        appConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(appConf);
        MiniDFSCluster hdfsCluster = builder.build();
        miniCluster = YARNCluster.getInstance().getCluster();
        appConf.set("fs.defaultFS", "hdfs://localhost:" + hdfsCluster.getNameNodePort());
        miniCluster.init(appConf);
        Cluster defaultConfig = Utils.parseYarnClusterConfig(configPath);
        for (Node n : defaultConfig.getNode()) {
            n.setClusterIp(MiniYARNCluster.getHostname());
        }
        defaultConfig.getMasterNode().setClusterIp(MiniYARNCluster.getHostname());
        configPath = "target" + File.separator + "localized-aoya-config.xml";
        Utils.writeYarnClusterConfig(configPath, defaultConfig);
        miniCluster.start();
        appConf = new YarnConfiguration(miniCluster.getConfig());
        appConf.set("fs.defaultFS", "hdfs://localhost:" + hdfsCluster.getNameNodePort());
        //TODO:why must I do this!? what is not being passed properly via environment variables???
        appConf.writeXml(new FileOutputStream("target" + File.separator + "yarn-site.xml"));

        //once the cluster is created, you can get its configuration
        //with the binding details to the cluster added from the minicluster
        FileSystem fs = FileSystem.get(appConf);
        Path instanceState = new Path(fs.getHomeDirectory(), AsterixYARNClient.CONF_DIR_REL + INSTANCE_NAME + "/");
        fs.delete(instanceState, true);
        Assert.assertFalse(fs.exists(instanceState));

        File outdir = new File(PATH_ACTUAL);
        outdir.mkdirs();
        return appConf;
    }

    public void tearDown() throws Exception {
        FileSystem fs = FileSystem.get(appConf);
        Path instance = new Path(fs.getHomeDirectory(), AsterixYARNClient.CONF_DIR_REL + "/");
        fs.delete(instance, true);
        miniCluster.close();
        File outdir = new File(PATH_ACTUAL);
        File[] files = outdir.listFiles();
        if (files == null || files.length == 0) {
            outdir.delete();
        }
    }
}