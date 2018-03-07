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

import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.client.NodeStatus;
import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.topology.ClusterTopology;
import org.apache.hyracks.api.topology.TopologyDefinitionParser;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.Assert;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import junit.framework.TestCase;

@SuppressWarnings("deprecation")
public class SchedulerTest extends TestCase {
    private static String TOPOLOGY_PATH = "src/test/resources/topology.xml";

    private ClusterTopology parseTopology() throws IOException, SAXException {
        FileReader fr = new FileReader(TOPOLOGY_PATH);
        InputSource in = new InputSource(fr);
        try {
            return TopologyDefinitionParser.parse(in);
        } finally {
            fr.close();
        }
    }

    /**
     * Test the scheduler for the case when the Hyracks cluster is the HDFS cluster
     *
     * @throws Exception
     */
    public void testSchedulerSimple() throws Exception {
        Map<String, NodeControllerInfo> ncNameToNcInfos =
                TestUtils.generateNodeControllerInfo(6, "nc", "10.0.0.", 5099, 5098, 5097);

        InputSplit[] fileSplits = new InputSplit[6];
        fileSplits[0] = new FileSplit(new Path("part-1"), 0, 0, new String[] { "10.0.0.1", "10.0.0.2", "10.0.0.3" });
        fileSplits[1] = new FileSplit(new Path("part-2"), 0, 0, new String[] { "10.0.0.3", "10.0.0.4", "10.0.0.5" });
        fileSplits[2] = new FileSplit(new Path("part-3"), 0, 0, new String[] { "10.0.0.4", "10.0.0.5", "10.0.0.6" });
        fileSplits[3] = new FileSplit(new Path("part-4"), 0, 0, new String[] { "10.0.0.2", "10.0.0.1", "10.0.0.6" });
        fileSplits[4] = new FileSplit(new Path("part-5"), 0, 0, new String[] { "10.0.0.3", "10.0.0.4", "10.0.0.5" });
        fileSplits[5] = new FileSplit(new Path("part-6"), 0, 0, new String[] { "10.0.0.2", "10.0.0.3", "10.0.0.5" });

        String[] expectedResults = new String[] { "nc1", "nc4", "nc6", "nc2", "nc3", "nc5" };

        Scheduler scheduler = new Scheduler(ncNameToNcInfos);
        String[] locationConstraints = scheduler.getLocationConstraints(fileSplits);
        for (int i = 0; i < locationConstraints.length; i++) {
            Assert.assertEquals(locationConstraints[i], expectedResults[i]);
        }

        ClusterTopology topology = parseTopology();
        scheduler = new Scheduler(ncNameToNcInfos, topology);
        locationConstraints = scheduler.getLocationConstraints(fileSplits);
        for (int i = 0; i < locationConstraints.length; i++) {
            Assert.assertEquals(locationConstraints[i], expectedResults[i]);
        }
    }

    /**
     * Test the case where the HDFS cluster is a larger than the Hyracks cluster
     *
     * @throws Exception
     */
    public void testSchedulerLargerHDFS() throws Exception {
        int dataPort = 5099;
        int resultPort = 5098;
        int messagingPort = 5097;
        Map<String, NodeControllerInfo> ncNameToNcInfos =
                TestUtils.generateNodeControllerInfo(4, "nc", "10.0.0.", dataPort, resultPort, messagingPort);
        ncNameToNcInfos.put("nc7",
                new NodeControllerInfo("nc7", NodeStatus.ACTIVE, new NetworkAddress("10.0.0.7", dataPort),
                        new NetworkAddress("10.0.0.5", resultPort), new NetworkAddress("10.0.0.5", messagingPort), 2));
        ncNameToNcInfos.put("nc12",
                new NodeControllerInfo("nc12", NodeStatus.ACTIVE, new NetworkAddress("10.0.0.12", dataPort),
                        new NetworkAddress("10.0.0.5", resultPort), new NetworkAddress("10.0.0.5", messagingPort), 2));

        InputSplit[] fileSplits = new InputSplit[12];
        fileSplits[0] = new FileSplit(new Path("part-1"), 0, 0, new String[] { "10.0.0.1", "10.0.0.2", "10.0.0.3" });
        fileSplits[1] = new FileSplit(new Path("part-2"), 0, 0, new String[] { "10.0.0.3", "10.0.0.4", "10.0.0.5" });
        fileSplits[2] = new FileSplit(new Path("part-3"), 0, 0, new String[] { "10.0.0.4", "10.0.0.5", "10.0.0.6" });
        fileSplits[3] = new FileSplit(new Path("part-4"), 0, 0, new String[] { "10.0.0.2", "10.0.0.1", "10.0.0.6" });
        fileSplits[4] = new FileSplit(new Path("part-5"), 0, 0, new String[] { "10.0.0.3", "10.0.0.4", "10.0.0.5" });
        fileSplits[5] = new FileSplit(new Path("part-6"), 0, 0, new String[] { "10.0.0.2", "10.0.0.3", "10.0.0.5" });
        fileSplits[6] = new FileSplit(new Path("part-7"), 0, 0, new String[] { "10.0.0.1", "10.0.0.2", "10.0.0.3" });
        fileSplits[7] = new FileSplit(new Path("part-8"), 0, 0, new String[] { "10.0.0.3", "10.0.0.4", "10.0.0.5" });
        fileSplits[8] =
                new FileSplit(new Path("part-12"), 0, 0, new String[] { "10.0.0.14", "10.0.0.11", "10.0.0.13" });
        fileSplits[9] = new FileSplit(new Path("part-10"), 0, 0, new String[] { "10.0.0.2", "10.0.0.1", "10.0.0.6" });
        fileSplits[10] = new FileSplit(new Path("part-11"), 0, 0, new String[] { "10.0.0.3", "10.0.0.4", "10.0.0.7" });
        fileSplits[11] = new FileSplit(new Path("part-9"), 0, 0, new String[] { "10.0.0.4", "10.0.0.5", "10.0.0.6" });

        Scheduler scheduler = new Scheduler(ncNameToNcInfos);
        String[] locationConstraints = scheduler.getLocationConstraints(fileSplits);

        String[] expectedResults =
                new String[] { "nc1", "nc4", "nc4", "nc1", "nc3", "nc2", "nc2", "nc3", "nc12", "nc7", "nc7", "nc12" };
        for (int i = 0; i < locationConstraints.length; i++) {
            Assert.assertEquals(locationConstraints[i], expectedResults[i]);
        }

        expectedResults =
                new String[] { "nc1", "nc4", "nc4", "nc1", "nc3", "nc2", "nc2", "nc3", "nc7", "nc12", "nc7", "nc12" };
        ClusterTopology topology = parseTopology();
        scheduler = new Scheduler(ncNameToNcInfos, topology);
        locationConstraints = scheduler.getLocationConstraints(fileSplits);
        for (int i = 0; i < locationConstraints.length; i++) {
            Assert.assertEquals(locationConstraints[i], expectedResults[i]);
        }
    }

    /**
     * Test the case where the HDFS cluster is a larger than the Hyracks cluster
     *
     * @throws Exception
     */
    public void testSchedulerSmallerHDFS() throws Exception {
        Map<String, NodeControllerInfo> ncNameToNcInfos =
                TestUtils.generateNodeControllerInfo(6, "nc", "10.0.0.", 5099, 5098, 5097);

        InputSplit[] fileSplits = new InputSplit[12];
        fileSplits[0] = new FileSplit(new Path("part-1"), 0, 0, new String[] { "10.0.0.1", "10.0.0.2", "10.0.0.3" });
        fileSplits[1] = new FileSplit(new Path("part-2"), 0, 0, new String[] { "10.0.0.3", "10.0.0.4", "10.0.0.5" });
        fileSplits[2] = new FileSplit(new Path("part-3"), 0, 0, new String[] { "10.0.0.4", "10.0.0.5", "10.0.0.3" });
        fileSplits[3] = new FileSplit(new Path("part-4"), 0, 0, new String[] { "10.0.0.2", "10.0.0.1", "10.0.0.3" });
        fileSplits[4] = new FileSplit(new Path("part-5"), 0, 0, new String[] { "10.0.0.3", "10.0.0.4", "10.0.0.5" });
        fileSplits[5] = new FileSplit(new Path("part-6"), 0, 0, new String[] { "10.0.0.2", "10.0.0.3", "10.0.0.5" });
        fileSplits[6] = new FileSplit(new Path("part-7"), 0, 0, new String[] { "10.0.0.1", "10.0.0.2", "10.0.0.3" });
        fileSplits[7] = new FileSplit(new Path("part-8"), 0, 0, new String[] { "10.0.0.3", "10.0.0.4", "10.0.0.5" });
        fileSplits[8] = new FileSplit(new Path("part-9"), 0, 0, new String[] { "10.0.0.4", "10.0.0.5", "10.0.0.1" });
        fileSplits[9] = new FileSplit(new Path("part-10"), 0, 0, new String[] { "10.0.0.2", "10.0.0.1", "10.0.0.2" });
        fileSplits[10] = new FileSplit(new Path("part-11"), 0, 0, new String[] { "10.0.0.3", "10.0.0.4", "10.0.0.5" });
        fileSplits[11] = new FileSplit(new Path("part-12"), 0, 0, new String[] { "10.0.0.2", "10.0.0.3", "10.0.0.5" });

        String[] expectedResults =
                new String[] { "nc1", "nc4", "nc4", "nc1", "nc3", "nc2", "nc2", "nc3", "nc5", "nc6", "nc5", "nc6" };

        Scheduler scheduler = new Scheduler(ncNameToNcInfos);
        String[] locationConstraints = scheduler.getLocationConstraints(fileSplits);

        for (int i = 0; i < locationConstraints.length; i++) {
            Assert.assertEquals(locationConstraints[i], expectedResults[i]);
        }

        ClusterTopology topology = parseTopology();
        scheduler = new Scheduler(ncNameToNcInfos, topology);
        locationConstraints = scheduler.getLocationConstraints(fileSplits);
        for (int i = 0; i < locationConstraints.length; i++) {
            Assert.assertEquals(locationConstraints[i], expectedResults[i]);
        }
    }

    /**
     * Test the case where the HDFS cluster is a larger than the Hyracks cluster
     *
     * @throws Exception
     */
    public void testSchedulerSmallerHDFSOdd() throws Exception {
        Map<String, NodeControllerInfo> ncNameToNcInfos =
                TestUtils.generateNodeControllerInfo(6, "nc", "10.0.0.", 5099, 5098, 5097);

        InputSplit[] fileSplits = new InputSplit[13];
        fileSplits[0] = new FileSplit(new Path("part-1"), 0, 0, new String[] { "10.0.0.1", "10.0.0.2", "10.0.0.3" });
        fileSplits[1] = new FileSplit(new Path("part-2"), 0, 0, new String[] { "10.0.0.3", "10.0.0.4", "10.0.0.5" });
        fileSplits[2] = new FileSplit(new Path("part-3"), 0, 0, new String[] { "10.0.0.4", "10.0.0.5", "10.0.0.3" });
        fileSplits[3] = new FileSplit(new Path("part-4"), 0, 0, new String[] { "10.0.0.2", "10.0.0.1", "10.0.0.3" });
        fileSplits[4] = new FileSplit(new Path("part-5"), 0, 0, new String[] { "10.0.0.3", "10.0.0.4", "10.0.0.5" });
        fileSplits[5] = new FileSplit(new Path("part-6"), 0, 0, new String[] { "10.0.0.2", "10.0.0.3", "10.0.0.5" });
        fileSplits[6] = new FileSplit(new Path("part-7"), 0, 0, new String[] { "10.0.0.1", "10.0.0.2", "10.0.0.3" });
        fileSplits[7] = new FileSplit(new Path("part-8"), 0, 0, new String[] { "10.0.0.3", "10.0.0.4", "10.0.0.5" });
        fileSplits[8] = new FileSplit(new Path("part-9"), 0, 0, new String[] { "10.0.0.4", "10.0.0.5", "10.0.0.1" });
        fileSplits[9] = new FileSplit(new Path("part-10"), 0, 0, new String[] { "10.0.0.2", "10.0.0.1", "10.0.0.2" });
        fileSplits[10] = new FileSplit(new Path("part-11"), 0, 0, new String[] { "10.0.0.3", "10.0.0.4", "10.0.0.5" });
        fileSplits[11] = new FileSplit(new Path("part-12"), 0, 0, new String[] { "10.0.0.2", "10.0.0.3", "10.0.0.5" });
        fileSplits[12] = new FileSplit(new Path("part-13"), 0, 0, new String[] { "10.0.0.2", "10.0.0.4", "10.0.0.5" });

        String[] expectedResults = new String[] { "nc1", "nc4", "nc4", "nc1", "nc3", "nc2", "nc2", "nc3", "nc5", "nc1",
                "nc5", "nc2", "nc4" };

        Scheduler scheduler = new Scheduler(ncNameToNcInfos);
        String[] locationConstraints = scheduler.getLocationConstraints(fileSplits);

        for (int i = 0; i < locationConstraints.length; i++) {
            Assert.assertEquals(locationConstraints[i], expectedResults[i]);
        }

        ClusterTopology topology = parseTopology();
        scheduler = new Scheduler(ncNameToNcInfos, topology);
        locationConstraints = scheduler.getLocationConstraints(fileSplits);
        for (int i = 0; i < locationConstraints.length; i++) {
            Assert.assertEquals(locationConstraints[i], expectedResults[i]);
        }

    }

    /**
     * Test boundary cases where splits array is empty or null
     *
     * @throws Exception
     */
    public void testSchedulercBoundary() throws Exception {
        Map<String, NodeControllerInfo> ncNameToNcInfos =
                TestUtils.generateNodeControllerInfo(6, "nc", "10.0.0.", 5099, 5098, 5097);

        /** test empty file splits */
        InputSplit[] fileSplits = new InputSplit[0];
        String[] expectedResults = new String[] {};

        Scheduler scheduler = new Scheduler(ncNameToNcInfos);
        String[] locationConstraints = scheduler.getLocationConstraints(fileSplits);

        for (int i = 0; i < locationConstraints.length; i++) {
            Assert.assertEquals(locationConstraints[i], expectedResults[i]);
        }

        ClusterTopology topology = parseTopology();
        scheduler = new Scheduler(ncNameToNcInfos, topology);
        locationConstraints = scheduler.getLocationConstraints(fileSplits);
        for (int i = 0; i < locationConstraints.length; i++) {
            Assert.assertEquals(locationConstraints[i], expectedResults[i]);
        }

        fileSplits = null;
        expectedResults = new String[] {};

        scheduler = new Scheduler(ncNameToNcInfos);
        locationConstraints = scheduler.getLocationConstraints(fileSplits);
        for (int i = 0; i < locationConstraints.length; i++) {
            Assert.assertEquals(locationConstraints[i], expectedResults[i]);
        }

        scheduler = new Scheduler(ncNameToNcInfos, topology);
        locationConstraints = scheduler.getLocationConstraints(fileSplits);
        for (int i = 0; i < locationConstraints.length; i++) {
            Assert.assertEquals(locationConstraints[i], expectedResults[i]);
        }

    }

}
