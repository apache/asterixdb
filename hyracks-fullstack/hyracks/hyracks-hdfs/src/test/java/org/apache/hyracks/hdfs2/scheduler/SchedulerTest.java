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

package org.apache.hyracks.hdfs2.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.Assert;

import junit.framework.TestCase;

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
        Map<String, NodeControllerInfo> ncNameToNcInfos =
                TestUtils.generateNodeControllerInfo(6, "nc", "10.0.0.", 5099, 5098, 5097);

        List<InputSplit> fileSplits = new ArrayList<>();
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
        Map<String, NodeControllerInfo> ncNameToNcInfos =
                TestUtils.generateNodeControllerInfo(6, "nc", "10.0.0.", 5099, 5098, 5097);

        List<InputSplit> fileSplits = new ArrayList<>();
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

        String[] expectedResults =
                new String[] { "nc1", "nc4", "nc6", "nc1", "nc4", "nc2", "nc2", "nc3", "nc6", "nc5", "nc3", "nc5" };

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

        List<InputSplit> fileSplits = new ArrayList<>();
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

        String[] expectedResults =
                new String[] { "nc1", "nc4", "nc4", "nc1", "nc3", "nc2", "nc2", "nc3", "nc5", "nc6", "nc5", "nc6" };

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

        List<InputSplit> fileSplits = new ArrayList<>();
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
