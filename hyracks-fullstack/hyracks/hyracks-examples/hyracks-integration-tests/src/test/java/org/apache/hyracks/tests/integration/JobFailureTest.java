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

import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.misc.SinkOperatorDescriptor;
import org.apache.hyracks.tests.util.ExceptionOnCreatePushRuntimeOperatorDescriptor;
import org.apache.hyracks.tests.util.FailOnInitializeDeInitializeOperatorDescriptor;
import org.junit.Assert;
import org.junit.Test;

public class JobFailureTest extends AbstractMultiNCIntegrationTest {

    @Test
    public void failureOnCreatePushRuntime() throws Exception {
        JobId jobId = null;
        for (int i = 0; i < 20; i++) {
            JobSpecification spec = new JobSpecification();
            JobId runJobId = runTest(spec,
                    new ExceptionOnCreatePushRuntimeOperatorDescriptor(spec, 0, 1, new int[] { 4 }, true));
            if (i == 0) {
                jobId = runJobId;
                // passes. read from job archive
                waitForCompletion(jobId, ExceptionOnCreatePushRuntimeOperatorDescriptor.ERROR_MESSAGE);
            }
        }
        // passes. read from job history
        waitForCompletion(jobId, ExceptionOnCreatePushRuntimeOperatorDescriptor.ERROR_MESSAGE);
        for (int i = 0; i < 300; i++) {
            JobSpecification spec = new JobSpecification();
            runTest(spec, new ExceptionOnCreatePushRuntimeOperatorDescriptor(spec, 0, 1, new int[] { 4 }, true));
        }
        // passes. history has been cleared
        waitForCompletion(jobId, "has been cleared from job history");
    }

    @Test
    public void failureOnInit() throws Exception {
        JobSpecification spec = new JobSpecification();
        connectToSinkAndRun(spec, new FailOnInitializeDeInitializeOperatorDescriptor(spec, true, false),
                FailOnInitializeDeInitializeOperatorDescriptor.INIT_ERROR_MESSAGE);
        // Ensure you can run the next job
        spec = new JobSpecification();
        connectToSinkAndRun(spec, new FailOnInitializeDeInitializeOperatorDescriptor(spec, false, false), null);
    }

    @Test
    public void failureOnDeinit() throws Exception {
        JobSpecification spec = new JobSpecification();
        connectToSinkAndRun(spec, new FailOnInitializeDeInitializeOperatorDescriptor(spec, false, true),
                FailOnInitializeDeInitializeOperatorDescriptor.DEINIT_ERROR_MESSAGE);
        // Ensure you can run the next job
        spec = new JobSpecification();
        connectToSinkAndRun(spec, new FailOnInitializeDeInitializeOperatorDescriptor(spec, false, false), null);
    }

    @Test
    public void failureOnInitDeinit() throws Exception {
        JobSpecification spec = new JobSpecification();
        connectToSinkAndRun(spec, new FailOnInitializeDeInitializeOperatorDescriptor(spec, true, true),
                FailOnInitializeDeInitializeOperatorDescriptor.INIT_ERROR_MESSAGE);
        // Ensure you can run the next job
        spec = new JobSpecification();
        connectToSinkAndRun(spec, new FailOnInitializeDeInitializeOperatorDescriptor(spec, false, false), null);
    }

    private JobId runTest(JobSpecification spec, AbstractSingleActivityOperatorDescriptor sourceOpDesc)
            throws Exception {
        try {
            return connectToSinkAndRun(spec, sourceOpDesc,
                    ExceptionOnCreatePushRuntimeOperatorDescriptor.ERROR_MESSAGE);
        } finally {
            Assert.assertTrue(
                    ExceptionOnCreatePushRuntimeOperatorDescriptor.stats()
                            + ExceptionOnCreatePushRuntimeOperatorDescriptor.succeed(),
                    ExceptionOnCreatePushRuntimeOperatorDescriptor.succeed());
            // should also check the content of the different ncs
        }
    }

    private JobId connectToSinkAndRun(JobSpecification spec, AbstractSingleActivityOperatorDescriptor sourceOpDesc,
            String expectedError) throws Exception {
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sourceOpDesc, ASTERIX_IDS);
        SinkOperatorDescriptor sinkOpDesc = new SinkOperatorDescriptor(spec, 1);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sinkOpDesc, ASTERIX_IDS);
        IConnectorDescriptor conn = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn, sourceOpDesc, 0, sinkOpDesc, 0);
        spec.addRoot(sinkOpDesc);
        try {
            return runTest(spec, expectedError);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
