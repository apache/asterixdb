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
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.misc.SinkOperatorDescriptor;
import org.apache.hyracks.tests.util.ExceptionOnCreatePushRuntimeOperatorDescriptor;
import org.junit.Assert;
import org.junit.Test;

public class JobFailureTest extends AbstractMultiNCIntegrationTest {

    @Test
    public void failureOnCreatePushRuntime() throws Exception {
        for (int round = 0; round < 1000; ++round) {
            execTest();
        }
    }

    private void execTest() throws Exception {
        JobSpecification spec = new JobSpecification();
        AbstractSingleActivityOperatorDescriptor sourceOpDesc = new ExceptionOnCreatePushRuntimeOperatorDescriptor(spec,
                0, 1, new int[] { 4 }, true);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sourceOpDesc, ASTERIX_IDS);
        SinkOperatorDescriptor sinkOpDesc = new SinkOperatorDescriptor(spec, 1);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sinkOpDesc, ASTERIX_IDS);
        IConnectorDescriptor conn = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn, sourceOpDesc, 0, sinkOpDesc, 0);
        spec.addRoot(sinkOpDesc);
        try {
            runTest(spec);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        Assert.assertTrue(ExceptionOnCreatePushRuntimeOperatorDescriptor.succeed());
        // should also check the content of the different ncs
    }
}
