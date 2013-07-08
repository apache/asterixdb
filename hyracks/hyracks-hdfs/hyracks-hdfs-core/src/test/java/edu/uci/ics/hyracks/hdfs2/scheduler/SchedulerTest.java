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

package edu.uci.ics.hyracks.hdfs2.scheduler;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.client.NodeStatus;
import edu.uci.ics.hyracks.api.comm.NetworkAddress;

/**
 * Test case for the new HDFS API scheduler
 */
public class SchedulerTest extends TestCase {

    /**
     * Test the scheduler for the case when the Hyracks cluster is the HDFS cluster
     * 
     * @throws Exception
     */
    public void testSchedulerSimple() throws Exception {
        Map<String, NodeControllerInfo> ncNameToNcInfos = new HashMap<String, NodeControllerInfo>();
        ncNameToNcInfos.put("nc1", new NodeControllerInfo("nc1", NodeStatus.ALIVE, new NetworkAddress(InetAddress
                .getByName("10.0.0.1").getAddress(), 5099), new NetworkAddress(InetAddress.getByName("10.0.0.1")
                .getAddress(), 5098)));
        ncNameToNcInfos.put("nc2", new NodeControllerInfo("nc2", NodeStatus.ALIVE, new NetworkAddress(InetAddress
                .getByName("10.0.0.2").getAddress(), 5099), new NetworkAddress(InetAddress.getByName("10.0.0.2")
                .getAddress(), 5098)));
        ncNameToNcInfos.put("nc3", new NodeControllerInfo("nc3", NodeStatus.ALIVE, new NetworkAddress(InetAddress
                .getByName("10.0.0.3").getAddress(), 5099), new NetworkAddress(InetAddress.getByName("10.0.0.3")
                .getAddress(), 5098)));
        ncNameToNcInfos.put("nc4", new NodeControllerInfo("nc4", NodeStatus.ALIVE, new NetworkAddress(InetAddress
                .getByName("10.0.0.4").getAddress(), 5099), new NetworkAddress(InetAddress.getByName("10.0.0.4")
                .getAddress(), 5098)));
        ncNameToNcInfos.put("nc5", new NodeControllerInfo("nc5", NodeStatus.ALIVE, new NetworkAddress(InetAddress
                .getByName("10.0.0.5").getAddress(), 5099), new NetworkAddress(InetAddress.getByName("10.0.0.5")
                .getAddress(), 5098)));
        ncNameToNcInfos.put("nc6", new NodeControllerInfo("nc6", NodeStatus.ALIVE, new NetworkAddress(InetAddress
                .getByName("10.0.0.6").getAddress(), 5099), new NetworkAddress(InetAddress.getByName("10.0.0.6")
                .getAddress(), 5098)));

        List<InputSplit> fileSplits = new ArrayList<InputSplit>();
        fileSplits.add(new FileSplit(new Path("part-1"), 0, 0, new String[] { "10.0.0.1", "10.0.0.2", "10.0.0.3" }));
        fileSplits.add(new FileSplit(new Path("part-2"), 0, 0, new String[] { "10.0.0.3", "10.0.0.4", "10.0.0.5" }));
        fileSplits.add(new FileSplit(new Path("part-3"), 0, 0, new String[] { "10.0.0.4", "10.0.0.5", "10.0.0.6" }));
        fileSplits.add(new FileSplit(new Path("part-4"), 0, 0, new String[] { "10.0.0.2", "10.0.0.1", "10.0.0.6" }));
        fileSplits.add(new FileSplit(new Path("part-5"), 0, 0, new String[] { "10.0.0.3", "10.0.0.4", "10.0.0.5" }));
        fileSplits.add(new FileSplit(new Path("part-6"), 0, 0, new String[] { "10.0.0.2", "10.0.0.3", "10.0.0.5" }));

        Scheduler scheduler = new Scheduler(ncNameToNcInfos);
        String[] locationConstraints = scheduler.getLocationConstraints(fileSplits);

        String[] expectedResults = new String[] { "nc1", "nc4", "nc6", "nc2", "nc3", "nc5" };

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
        Map<String, NodeControllerInfo> ncNameToNcInfos = new HashMap<String, NodeControllerInfo>();
        ncNameToNcInfos.put("nc1", new NodeControllerInfo("nc1", NodeStatus.ALIVE, new NetworkAddress(InetAddress
                .getByName("10.0.0.1").getAddress(), 5099), new NetworkAddress(InetAddress.getByName("10.0.0.1")
                .getAddress(), 5098)));
        ncNameToNcInfos.put("nc2", new NodeControllerInfo("nc2", NodeStatus.ALIVE, new NetworkAddress(InetAddress
                .getByName("10.0.0.2").getAddress(), 5099), new NetworkAddress(InetAddress.getByName("10.0.0.2")
                .getAddress(), 5098)));
        ncNameToNcInfos.put("nc3", new NodeControllerInfo("nc3", NodeStatus.ALIVE, new NetworkAddress(InetAddress
                .getByName("10.0.0.3").getAddress(), 5099), new NetworkAddress(InetAddress.getByName("10.0.0.3")
                .getAddress(), 5098)));
        ncNameToNcInfos.put("nc4", new NodeControllerInfo("nc4", NodeStatus.ALIVE, new NetworkAddress(InetAddress
                .getByName("10.0.0.4").getAddress(), 5099), new NetworkAddress(InetAddress.getByName("10.0.0.4")
                .getAddress(), 5098)));
        ncNameToNcInfos.put("nc5", new NodeControllerInfo("nc5", NodeStatus.ALIVE, new NetworkAddress(InetAddress
                .getByName("10.0.0.5").getAddress(), 5099), new NetworkAddress(InetAddress.getByName("10.0.0.5")
                .getAddress(), 5098)));
        ncNameToNcInfos.put("nc6", new NodeControllerInfo("nc6", NodeStatus.ALIVE, new NetworkAddress(InetAddress
                .getByName("10.0.0.6").getAddress(), 5099), new NetworkAddress(InetAddress.getByName("10.0.0.6")
                .getAddress(), 5098)));

        List<InputSplit> fileSplits = new ArrayList<InputSplit>();
        fileSplits.add(new FileSplit(new Path("part-1"), 0, 0, new String[] { "10.0.0.1", "10.0.0.2", "10.0.0.3" }));
        fileSplits.add(new FileSplit(new Path("part-2"), 0, 0, new String[] { "10.0.0.3", "10.0.0.4", "10.0.0.5" }));
        fileSplits.add(new FileSplit(new Path("part-3"), 0, 0, new String[] { "10.0.0.4", "10.0.0.5", "10.0.0.6" }));
        fileSplits.add(new FileSplit(new Path("part-4"), 0, 0, new String[] { "10.0.0.2", "10.0.0.1", "10.0.0.6" }));
        fileSplits.add(new FileSplit(new Path("part-5"), 0, 0, new String[] { "10.0.0.3", "10.0.0.4", "10.0.0.5" }));
        fileSplits.add(new FileSplit(new Path("part-6"), 0, 0, new String[] { "10.0.0.2", "10.0.0.3", "10.0.0.5" }));
        fileSplits.add(new FileSplit(new Path("part-7"), 0, 0, new String[] { "10.0.0.1", "10.0.0.2", "10.0.0.3" }));
        fileSplits.add(new FileSplit(new Path("part-8"), 0, 0, new String[] { "10.0.0.3", "10.0.0.4", "10.0.0.5" }));
        fileSplits.add(new FileSplit(new Path("part-9"), 0, 0, new String[] { "10.0.0.4", "10.0.0.5", "10.0.0.6" }));
        fileSplits.add(new FileSplit(new Path("part-10"), 0, 0, new String[] { "10.0.0.2", "10.0.0.1", "10.0.0.6" }));
        fileSplits.add(new FileSplit(new Path("part-11"), 0, 0, new String[] { "10.0.0.3", "10.0.0.4", "10.0.0.7" }));
        fileSplits.add(new FileSplit(new Path("part-12"), 0, 0, new String[] { "10.0.0.2", "10.0.0.3", "10.0.0.5" }));

        Scheduler scheduler = new Scheduler(ncNameToNcInfos);
        String[] locationConstraints = scheduler.getLocationConstraints(fileSplits);

        String[] expectedResults = new String[] { "nc1", "nc4", "nc6", "nc1", "nc4", "nc2", "nc2", "nc3", "nc6", "nc5",
                "nc3", "nc5" };

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
        Map<String, NodeControllerInfo> ncNameToNcInfos = new HashMap<String, NodeControllerInfo>();
        ncNameToNcInfos.put("nc1", new NodeControllerInfo("nc1", NodeStatus.ALIVE, new NetworkAddress(InetAddress
                .getByName("10.0.0.1").getAddress(), 5099), new NetworkAddress(InetAddress.getByName("10.0.0.1")
                .getAddress(), 5098)));
        ncNameToNcInfos.put("nc2", new NodeControllerInfo("nc2", NodeStatus.ALIVE, new NetworkAddress(InetAddress
                .getByName("10.0.0.2").getAddress(), 5099), new NetworkAddress(InetAddress.getByName("10.0.0.2")
                .getAddress(), 5098)));
        ncNameToNcInfos.put("nc3", new NodeControllerInfo("nc3", NodeStatus.ALIVE, new NetworkAddress(InetAddress
                .getByName("10.0.0.3").getAddress(), 5099), new NetworkAddress(InetAddress.getByName("10.0.0.3")
                .getAddress(), 5098)));
        ncNameToNcInfos.put("nc4", new NodeControllerInfo("nc4", NodeStatus.ALIVE, new NetworkAddress(InetAddress
                .getByName("10.0.0.4").getAddress(), 5099), new NetworkAddress(InetAddress.getByName("10.0.0.4")
                .getAddress(), 5098)));
        ncNameToNcInfos.put("nc5", new NodeControllerInfo("nc5", NodeStatus.ALIVE, new NetworkAddress(InetAddress
                .getByName("10.0.0.5").getAddress(), 5099), new NetworkAddress(InetAddress.getByName("10.0.0.5")
                .getAddress(), 5098)));
        ncNameToNcInfos.put("nc6", new NodeControllerInfo("nc6", NodeStatus.ALIVE, new NetworkAddress(InetAddress
                .getByName("10.0.0.6").getAddress(), 5099), new NetworkAddress(InetAddress.getByName("10.0.0.6")
                .getAddress(), 5098)));

        List<InputSplit> fileSplits = new ArrayList<InputSplit>();
        fileSplits.add(new FileSplit(new Path("part-1"), 0, 0, new String[] { "10.0.0.1", "10.0.0.2", "10.0.0.3" }));
        fileSplits.add(new FileSplit(new Path("part-2"), 0, 0, new String[] { "10.0.0.3", "10.0.0.4", "10.0.0.5" }));
        fileSplits.add(new FileSplit(new Path("part-3"), 0, 0, new String[] { "10.0.0.4", "10.0.0.5", "10.0.0.3" }));
        fileSplits.add(new FileSplit(new Path("part-4"), 0, 0, new String[] { "10.0.0.2", "10.0.0.1", "10.0.0.3" }));
        fileSplits.add(new FileSplit(new Path("part-5"), 0, 0, new String[] { "10.0.0.3", "10.0.0.4", "10.0.0.5" }));
        fileSplits.add(new FileSplit(new Path("part-6"), 0, 0, new String[] { "10.0.0.2", "10.0.0.3", "10.0.0.5" }));
        fileSplits.add(new FileSplit(new Path("part-7"), 0, 0, new String[] { "10.0.0.1", "10.0.0.2", "10.0.0.3" }));
        fileSplits.add(new FileSplit(new Path("part-8"), 0, 0, new String[] { "10.0.0.3", "10.0.0.4", "10.0.0.5" }));
        fileSplits.add(new FileSplit(new Path("part-9"), 0, 0, new String[] { "10.0.0.4", "10.0.0.5", "10.0.0.1" }));
        fileSplits.add(new FileSplit(new Path("part-10"), 0, 0, new String[] { "10.0.0.2", "10.0.0.1", "10.0.0.2" }));
        fileSplits.add(new FileSplit(new Path("part-11"), 0, 0, new String[] { "10.0.0.3", "10.0.0.4", "10.0.0.5" }));
        fileSplits.add(new FileSplit(new Path("part-12"), 0, 0, new String[] { "10.0.0.2", "10.0.0.3", "10.0.0.5" }));

        Scheduler scheduler = new Scheduler(ncNameToNcInfos);
        String[] locationConstraints = scheduler.getLocationConstraints(fileSplits);

        String[] expectedResults = new String[] { "nc1", "nc4", "nc4", "nc1", "nc3", "nc2", "nc2", "nc3", "nc5", "nc6",
                "nc5", "nc6" };

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
        Map<String, NodeControllerInfo> ncNameToNcInfos = new HashMap<String, NodeControllerInfo>();
        ncNameToNcInfos.put("nc1", new NodeControllerInfo("nc1", NodeStatus.ALIVE, new NetworkAddress(InetAddress
                .getByName("10.0.0.1").getAddress(), 5099), new NetworkAddress(InetAddress.getByName("10.0.0.1")
                .getAddress(), 5098)));
        ncNameToNcInfos.put("nc2", new NodeControllerInfo("nc2", NodeStatus.ALIVE, new NetworkAddress(InetAddress
                .getByName("10.0.0.2").getAddress(), 5099), new NetworkAddress(InetAddress.getByName("10.0.0.2")
                .getAddress(), 5098)));
        ncNameToNcInfos.put("nc3", new NodeControllerInfo("nc3", NodeStatus.ALIVE, new NetworkAddress(InetAddress
                .getByName("10.0.0.3").getAddress(), 5099), new NetworkAddress(InetAddress.getByName("10.0.0.3")
                .getAddress(), 5098)));
        ncNameToNcInfos.put("nc4", new NodeControllerInfo("nc4", NodeStatus.ALIVE, new NetworkAddress(InetAddress
                .getByName("10.0.0.4").getAddress(), 5099), new NetworkAddress(InetAddress.getByName("10.0.0.4")
                .getAddress(), 5098)));
        ncNameToNcInfos.put("nc5", new NodeControllerInfo("nc5", NodeStatus.ALIVE, new NetworkAddress(InetAddress
                .getByName("10.0.0.5").getAddress(), 5099), new NetworkAddress(InetAddress.getByName("10.0.0.5")
                .getAddress(), 5098)));
        ncNameToNcInfos.put("nc6", new NodeControllerInfo("nc6", NodeStatus.ALIVE, new NetworkAddress(InetAddress
                .getByName("10.0.0.6").getAddress(), 5099), new NetworkAddress(InetAddress.getByName("10.0.0.6")
                .getAddress(), 5098)));

        List<InputSplit> fileSplits = new ArrayList<InputSplit>();
        fileSplits.add(new FileSplit(new Path("part-1"), 0, 0, new String[] { "10.0.0.1", "10.0.0.2", "10.0.0.3" }));
        fileSplits.add(new FileSplit(new Path("part-2"), 0, 0, new String[] { "10.0.0.3", "10.0.0.4", "10.0.0.5" }));
        fileSplits.add(new FileSplit(new Path("part-3"), 0, 0, new String[] { "10.0.0.4", "10.0.0.5", "10.0.0.3" }));
        fileSplits.add(new FileSplit(new Path("part-4"), 0, 0, new String[] { "10.0.0.2", "10.0.0.1", "10.0.0.3" }));
        fileSplits.add(new FileSplit(new Path("part-5"), 0, 0, new String[] { "10.0.0.3", "10.0.0.4", "10.0.0.5" }));
        fileSplits.add(new FileSplit(new Path("part-6"), 0, 0, new String[] { "10.0.0.2", "10.0.0.3", "10.0.0.5" }));
        fileSplits.add(new FileSplit(new Path("part-7"), 0, 0, new String[] { "10.0.0.1", "10.0.0.2", "10.0.0.3" }));
        fileSplits.add(new FileSplit(new Path("part-8"), 0, 0, new String[] { "10.0.0.3", "10.0.0.4", "10.0.0.5" }));
        fileSplits.add(new FileSplit(new Path("part-9"), 0, 0, new String[] { "10.0.0.4", "10.0.0.5", "10.0.0.1" }));
        fileSplits.add(new FileSplit(new Path("part-10"), 0, 0, new String[] { "10.0.0.2", "10.0.0.1", "10.0.0.2" }));
        fileSplits.add(new FileSplit(new Path("part-11"), 0, 0, new String[] { "10.0.0.3", "10.0.0.4", "10.0.0.5" }));
        fileSplits.add(new FileSplit(new Path("part-12"), 0, 0, new String[] { "10.0.0.2", "10.0.0.3", "10.0.0.5" }));
        fileSplits.add(new FileSplit(new Path("part-13"), 0, 0, new String[] { "10.0.0.2", "10.0.0.4", "10.0.0.5" }));

        Scheduler scheduler = new Scheduler(ncNameToNcInfos);
        String[] locationConstraints = scheduler.getLocationConstraints(fileSplits);

        String[] expectedResults = new String[] { "nc1", "nc4", "nc4", "nc1", "nc3", "nc2", "nc2", "nc3", "nc5", "nc1",
                "nc5", "nc2", "nc4" };

        for (int i = 0; i < locationConstraints.length; i++) {
            Assert.assertEquals(locationConstraints[i], expectedResults[i]);
        }
    }

}
