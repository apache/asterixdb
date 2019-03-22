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
package org.apache.asterix.runtime.job.listener;

import java.util.Collections;

import org.apache.hyracks.api.constraints.Constraint;
import org.apache.hyracks.api.constraints.expressions.ConstantExpression;
import org.apache.hyracks.api.constraints.expressions.LValueConstraintExpression;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class NodeJobTrackerTest {

    @Test
    public void hasPendingJobsTest() {
        final String nc1 = "nc1";
        final String nc2 = "nc2";
        final String unknown = "unknown";
        final NodeJobTracker nodeJobTracker = new NodeJobTracker();
        nodeJobTracker.notifyNodeJoin(nc1, null);
        nodeJobTracker.notifyNodeJoin(nc2, null);

        JobSpecification jobSpec = new JobSpecification();
        // add nc1 and some other unknown location
        final ConstantExpression nc1Location = new ConstantExpression(nc1);
        final ConstantExpression unknownLocation = new ConstantExpression(unknown);
        final LValueConstraintExpression lValueMock = Mockito.mock(LValueConstraintExpression.class);
        jobSpec.getUserConstraints().add(new Constraint(lValueMock, nc1Location));
        jobSpec.getUserConstraints().add(new Constraint(lValueMock, unknownLocation));

        JobId jobId = new JobId(1);
        nodeJobTracker.notifyJobCreation(jobId, jobSpec);
        // make sure nc1 has a pending job
        Assert.assertTrue(nodeJobTracker.getPendingJobs(nc1).size() == 1);
        Assert.assertTrue(nodeJobTracker.getPendingJobs(unknown).isEmpty());
        Assert.assertTrue(nodeJobTracker.getPendingJobs(nc2).isEmpty());
        nodeJobTracker.notifyJobFinish(jobId, JobStatus.TERMINATED, null);
        // make sure nc1 doesn't have pending jobs anymore
        Assert.assertTrue(nodeJobTracker.getPendingJobs(nc1).isEmpty());

        // make sure node doesn't have pending jobs after failure
        jobId = new JobId(2);
        nodeJobTracker.notifyJobCreation(jobId, jobSpec);
        Assert.assertTrue(nodeJobTracker.getPendingJobs(nc1).size() == 1);
        nodeJobTracker.notifyNodeFailure(Collections.singleton(nc1));
        Assert.assertTrue(nodeJobTracker.getPendingJobs(nc1).isEmpty());
    }
}
