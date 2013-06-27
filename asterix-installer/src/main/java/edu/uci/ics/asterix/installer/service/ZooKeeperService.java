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
package edu.uci.ics.asterix.installer.service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import edu.uci.ics.asterix.installer.driver.InstallerDriver;
import edu.uci.ics.asterix.installer.driver.InstallerUtil;
import edu.uci.ics.asterix.installer.error.InstallerException;
import edu.uci.ics.asterix.installer.model.AsterixInstance;
import edu.uci.ics.asterix.installer.schema.conf.Configuration;

public class ZooKeeperService implements ILookupService {

    private static final Logger LOGGER = Logger.getLogger(ZooKeeperService.class.getName());

    private static final int ZOOKEEPER_LEADER_CONN_PORT = 2222;
    private static final int ZOOKEEPER_LEADER_ELEC_PORT = 2223;
    private static final int ZOOKEEPER_SESSION_TIME_OUT = 40 * 1000; //milliseconds
    private static final String ZOOKEEPER_HOME = InstallerDriver.getManagixHome() + File.separator
            + InstallerDriver.MANAGIX_INTERNAL_DIR + File.separator + "zookeeper";
    private static final String ZOO_KEEPER_CONFIG = ZOOKEEPER_HOME + File.separator + "zk.cfg";

    private boolean isRunning = false;
    private ZooKeeper zk;
    private String zkConnectionString;
    private static final String ASTERIX_INSTANCE_BASE_PATH = "/Asterix";
    private static final int DEFAULT_NODE_VERSION = -1;
    private LinkedBlockingQueue<String> msgQ = new LinkedBlockingQueue<String>();
    private ZooKeeperWatcher watcher = new ZooKeeperWatcher(msgQ);

    public boolean isRunning(Configuration conf) throws Exception {
        List<String> servers = conf.getZookeeper().getServers().getServer();
        int clientPort = conf.getZookeeper().getClientPort().intValue();
        StringBuffer connectionString = new StringBuffer();
        for (String serverAddress : servers) {
            connectionString.append(serverAddress);
            connectionString.append(":");
            connectionString.append(clientPort);
            connectionString.append(",");
        }
        if (connectionString.length() > 0) {
            connectionString.deleteCharAt(connectionString.length() - 1);
        }
        zkConnectionString = connectionString.toString();

        zk = new ZooKeeper(zkConnectionString, ZOOKEEPER_SESSION_TIME_OUT, watcher);
        try {
            zk.exists("/dummy", watcher);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("ZooKeeper running at " + connectionString);
            }
            createRootIfNotExist();
            isRunning = true;
        } catch (KeeperException ke) {
            isRunning = false;
        }
        return isRunning;
    }

    public void startService(Configuration conf) throws Exception {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Starting ZooKeeper at " + zkConnectionString);
        }
        ZookeeperUtil.writeConfiguration(ZOO_KEEPER_CONFIG, conf, ZOOKEEPER_LEADER_CONN_PORT,
                ZOOKEEPER_LEADER_ELEC_PORT);
        String initScript = ZOOKEEPER_HOME + File.separator + "bin" + File.separator + "zk.init";
        StringBuffer cmdBuffer = new StringBuffer();
        cmdBuffer.append(initScript + " ");
        cmdBuffer.append(conf.getZookeeper().getHomeDir() + " ");
        cmdBuffer.append(conf.getZookeeper().getServers().getJavaHome() + " ");
        List<String> zkServers = conf.getZookeeper().getServers().getServer();
        for (String zkServer : zkServers) {
            cmdBuffer.append(zkServer + " ");
        }
        Runtime.getRuntime().exec(cmdBuffer.toString());
        zk = new ZooKeeper(zkConnectionString, ZOOKEEPER_SESSION_TIME_OUT, watcher);
        String head = msgQ.poll(10, TimeUnit.SECONDS);
        if (head == null) {
            StringBuilder msg = new StringBuilder(
                    "Unable to start Zookeeper Service. This could be because of the following reasons.\n");
            msg.append("1) Managix is incorrectly configured. Please run " + InstallerDriver.getManagixHome()
                    + "/bin/managix validate" + " to run a validation test and correct the errors reported.");
            msg.append("\n2) If validation in (1) is successful, ensure that java_home parameter is set correctly in Managix configuration ("
                    + InstallerDriver.getManagixHome() + File.separator + InstallerDriver.MANAGIX_CONF_XML + ")");
            throw new Exception(msg.toString());
        }
        msgQ.take();
        createRootIfNotExist();
    }

    public void stopService(Configuration conf) throws Exception {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Stopping ZooKeeper running at " + zkConnectionString);
        }
        String stopScript = ZOOKEEPER_HOME + File.separator + "bin" + File.separator + "stop_zk";
        StringBuffer cmdBuffer = new StringBuffer();
        cmdBuffer.append(stopScript + " ");
        cmdBuffer.append(conf.getZookeeper().getHomeDir() + " ");
        List<String> zkServers = conf.getZookeeper().getServers().getServer();
        for (String zkServer : zkServers) {
            cmdBuffer.append(zkServer + " ");
        }
        Runtime.getRuntime().exec(cmdBuffer.toString());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Stopped ZooKeeper service at " + zkConnectionString);
        }
    }

    public void writeAsterixInstance(AsterixInstance asterixInstance) throws Exception {
        String instanceBasePath = ASTERIX_INSTANCE_BASE_PATH + File.separator + asterixInstance.getName();
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeObject(asterixInstance);
        zk.create(instanceBasePath, b.toByteArray(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    private void createRootIfNotExist() throws Exception {
        try {
            Stat stat = zk.exists(ASTERIX_INSTANCE_BASE_PATH, false);
            if (stat == null) {
                zk.create(ASTERIX_INSTANCE_BASE_PATH, "root".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (Exception e) {
            createRootIfNotExist();
        }
    }

    public AsterixInstance getAsterixInstance(String name) throws Exception {
        String path = ASTERIX_INSTANCE_BASE_PATH + File.separator + name;
        Stat stat = zk.exists(ASTERIX_INSTANCE_BASE_PATH + File.separator + name, false);
        if (stat == null) {
            return null;
        }
        byte[] asterixInstanceBytes = zk.getData(path, false, new Stat());
        return readAsterixInstanceObject(asterixInstanceBytes);
    }

    public boolean exists(String asterixInstanceName) throws Exception {
        return zk.exists(ASTERIX_INSTANCE_BASE_PATH + File.separator + asterixInstanceName, false) != null;
    }

    public void removeAsterixInstance(String name) throws Exception {
        if (!exists(name)) {
            throw new InstallerException("Asterix instance by name " + name + " does not exists.");
        }
        zk.delete(ASTERIX_INSTANCE_BASE_PATH + File.separator + name, DEFAULT_NODE_VERSION);
    }

    public List<AsterixInstance> getAsterixInstances() throws Exception {
        List<String> instanceNames = zk.getChildren(ASTERIX_INSTANCE_BASE_PATH, false);
        List<AsterixInstance> asterixInstances = new ArrayList<AsterixInstance>();
        String path;
        for (String instanceName : instanceNames) {
            path = ASTERIX_INSTANCE_BASE_PATH + File.separator + instanceName;
            byte[] asterixInstanceBytes = zk.getData(path, false, new Stat());
            asterixInstances.add(readAsterixInstanceObject(asterixInstanceBytes));
        }
        return asterixInstances;
    }

    private AsterixInstance readAsterixInstanceObject(byte[] asterixInstanceBytes) throws IOException,
            ClassNotFoundException {
        ByteArrayInputStream b = new ByteArrayInputStream(asterixInstanceBytes);
        ObjectInputStream ois = new ObjectInputStream(b);
        return (AsterixInstance) ois.readObject();
    }

    public void updateAsterixInstance(AsterixInstance updatedInstance) throws Exception {
        removeAsterixInstance(updatedInstance.getName());
        writeAsterixInstance(updatedInstance);
    }

}

class ZooKeeperWatcher implements Watcher {

    private boolean isRunning = true;
    private LinkedBlockingQueue<String> msgQ;

    public ZooKeeperWatcher(LinkedBlockingQueue<String> msgQ) {
        this.msgQ = msgQ;
    }

    public void process(WatchedEvent wEvent) {
        switch (wEvent.getState()) {
            case SyncConnected:
                msgQ.add("connected");
                break;
        }
    }

    public boolean isRunning() {
        return isRunning;
    }

}

class ZookeeperUtil {

    public static void writeConfiguration(String zooKeeperConfigPath, Configuration conf, int leaderConnPort,
            int leaderElecPort) throws IOException {

        StringBuffer buffer = new StringBuffer();
        buffer.append("tickTime=1000" + "\n");
        buffer.append("dataDir=" + conf.getZookeeper().getHomeDir() + File.separator + "data" + "\n");
        buffer.append("clientPort=" + conf.getZookeeper().getClientPort().intValue() + "\n");
        buffer.append("initLimit=" + 2 + "\n");
        buffer.append("syncLimit=" + 2 + "\n");

        List<String> servers = conf.getZookeeper().getServers().getServer();
        int serverId = 1;
        for (String server : servers) {
            buffer.append("server" + "." + serverId + "=" + server + ":" + leaderConnPort + ":" + leaderElecPort + "\n");
            serverId++;
        }
        InstallerUtil.dumpToFile(zooKeeperConfigPath, buffer.toString());
    }

}
