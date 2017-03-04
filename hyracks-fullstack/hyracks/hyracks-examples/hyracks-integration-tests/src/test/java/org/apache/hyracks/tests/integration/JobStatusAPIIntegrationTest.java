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
package org.apache.hyracks.tests.integration;

import static org.apache.hyracks.tests.integration.TestUtil.httpGetAsObject;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JobStatusAPIIntegrationTest extends AbstractIntegrationTest {

    public static final String ROOT_PATH = "/rest/jobs";

    @Test
    public void testNoRunningJobs() throws Exception {
        Assert.assertEquals(0, countJobs("RUNNING"));
    }

    @Test
    public void testRunningJob() throws Exception {
        JobId jId = startJob(); // startJob also checks that the job is running
        stopJob(jId);
        Assert.assertEquals("TERMINATED", getJobStatus(jId));
    }

    @Test
    public void testJobActivityGraph() throws Exception {
        JobId jId = startJob();
        ObjectNode res = getJobActivityGraph(jId);
        Assert.assertTrue(res.has("result"));
        ObjectNode actGraph = (ObjectNode) res.get("result");
        Assert.assertTrue(actGraph.has("version"));
        checkActivityCluster(actGraph);
        stopJob(jId);
        Assert.assertEquals("TERMINATED", getJobStatus(jId));
    }

    @Test
    public void testJobRun() throws Exception {
        JobId jId = startJob();
        ObjectNode res = getJobRun(jId);
        Assert.assertTrue(res.has("result"));
        ObjectNode jobRun = (ObjectNode) res.get("result");
        Assert.assertTrue(jobRun.has("job-id"));
        Assert.assertTrue(JobId.parse(jobRun.get("job-id").asText()).equals(jId));
        checkActivityCluster(jobRun);
        stopJob(jId);
        Assert.assertEquals("TERMINATED", getJobStatus(jId));
    }

    protected void checkActivityCluster(ObjectNode result) {
        Assert.assertTrue(result.has("activity-clusters"));
        ArrayNode actClusters = (ArrayNode) result.get("activity-clusters");
        Assert.assertEquals(1, actClusters.size());
        ObjectNode actCluster = (ObjectNode) actClusters.get(0);
        Assert.assertTrue(actCluster.has("activities"));
    }

    protected JobId startJob() throws Exception {
        WaitingOperatorDescriptor.CONTINUE_RUNNING.setFalse();
        JobSpecification spec = new JobSpecification();
        WaitingOperatorDescriptor sourceOpDesc = new WaitingOperatorDescriptor(spec, 0, 0);
        PartitionConstraintHelper.addPartitionCountConstraint(spec, sourceOpDesc, 1);
        spec.addRoot(sourceOpDesc);
        JobId jId = executeTest(spec);
        // don't run for more than 100 s
        int maxLoops = 1000;
        while (maxLoops > 0 && !"RUNNING".equals(getJobStatus(jId))) {
            Thread.sleep(100);
            --maxLoops;
        }
        return jId;
    }

    protected void stopJob(JobId jId) throws Exception {
        synchronized (WaitingOperatorDescriptor.CONTINUE_RUNNING) {
            WaitingOperatorDescriptor.CONTINUE_RUNNING.setTrue();
            WaitingOperatorDescriptor.CONTINUE_RUNNING.notify();
        }
        hcc.waitForCompletion(jId);
    }

    private int countJobs(String status) throws IOException, URISyntaxException {
        int res = 0;
        ArrayNode jobArray = getJobs();
        for (JsonNode n : jobArray) {
            ObjectNode o = (ObjectNode) n;
            if (status.equals(o.get("status").asText())) {
                ++res;
            }
        }
        return res;
    }

    private ObjectNode getJobSummary(JobId jId) throws IOException, URISyntaxException {
        ArrayNode jobArray = getJobs();
        for (JsonNode n : jobArray) {
            ObjectNode o = (ObjectNode) n;
            if (JobId.parse(o.get("job-id").asText()).equals(jId)) {
                return o;
            }
        }
        return null;
    }

    private String getJobStatus(JobId jId) throws IOException, URISyntaxException {
        ObjectNode o = getJobSummary(jId);
        return o != null ? o.get("status").asText() : null;
    }

    protected ArrayNode getJobs() throws URISyntaxException, IOException {
        return ((ArrayNode) httpGetAsObject(ROOT_PATH).get("result"));
    }

    protected ObjectNode getJobActivityGraph(JobId jId) throws URISyntaxException, IOException {
        ObjectNode o = getJobSummary(jId);
        URI uri = new URI(o.get("job-activity-graph").asText());
        return httpGetAsObject(uri);
    }

    protected ObjectNode getJobRun(JobId jId) throws URISyntaxException, IOException {
        ObjectNode o = getJobSummary(jId);
        URI uri = new URI(o.get("job-run").asText());
        return httpGetAsObject(uri);
    }
}
