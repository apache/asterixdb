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
package org.apache.asterix.event.service;

import java.io.File;
import java.util.List;

import org.apache.asterix.common.api.IClusterManagementWork.ClusterState;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

//A zookeeper watcher that watches the change in the state of the cluster
public class ClusterStateWatcher implements Watcher {
    private static Integer mutex;
    private static ZooKeeper zk;
    private String clusterStatePath;
    private boolean done = false;
    private ClusterState clusterState = ClusterState.STARTING;
    private boolean failed = false;
    private Exception failureCause = null;
    private static Logger LOGGER = Logger.getLogger(ClusterStateWatcher.class.getName());

    public ClusterStateWatcher(ZooKeeper zk, String clusterName) {
        if (mutex == null) {
            mutex = new Integer(-1);
        }
        this.clusterStatePath = ZooKeeperService.ASTERIX_INSTANCE_BASE_PATH + File.separator + clusterName
                + ZooKeeperService.ASTERIX_INSTANCE_STATE_PATH;
        ClusterStateWatcher.zk = zk;
    }

    public ClusterState waitForClusterStart() throws Exception {
        while (true) {
            synchronized (mutex) {
                if (done) {
                    if (failed) {
                        LOGGER.error("An error took place in the startup sequence. Check the CC logs.");
                        throw failureCause;
                    } else {
                        return clusterState;
                    }
                } else {
                    mutex.wait();
                }
            }
        }
    }

    private void monitorStateChange() {
        try {
            while (true) {
                synchronized (mutex) {
                    // Get the cluster state
                    List<String> list = zk.getChildren(clusterStatePath, this);
                    if (list.size() == 0) {
                        // Cluster state not found, wait to be awaken by Zookeeper
                        mutex.wait();
                    } else {
                        // Cluster state found
                        byte[] b = zk.getData(clusterStatePath + ZooKeeperService.ASTERIX_INSTANCE_STATE_REPORT, false,
                                null);
                        zk.delete(clusterStatePath + ZooKeeperService.ASTERIX_INSTANCE_STATE_REPORT, 0);
                        clusterState = ClusterState.values()[(int) b[0]];
                        done = true;
                        mutex.notifyAll();
                        return;
                    }
                }
            }
        } catch (Exception e) {
            // Exception was thrown, let Managix know that a failure took place
            failed = true;
            done = true;
            failureCause = e;
        }
    }

    public void startMonitoringThread() {
        Runnable monitoringThread = new Runnable() {
            @Override
            public void run() {
                monitorStateChange();
            }
        };
        // Start the monitoring thread
        (new Thread(monitoringThread)).start();
    }

    @Override
    synchronized public void process(WatchedEvent event) {
        synchronized (mutex) {
            mutex.notifyAll();
        }
    }
}