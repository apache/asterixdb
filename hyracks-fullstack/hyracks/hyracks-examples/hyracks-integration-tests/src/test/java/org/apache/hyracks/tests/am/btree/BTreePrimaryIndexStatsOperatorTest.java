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

package org.apache.hyracks.tests.am.btree;

import org.junit.Before;
import org.junit.Test;

import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.FileSplit;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.TreeIndexStatsOperatorDescriptor;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;

public class BTreePrimaryIndexStatsOperatorTest extends AbstractBTreeOperatorTest {

    @Before
    public void setup() throws Exception {
        super.setup();
        createPrimaryIndex();
        loadPrimaryIndex();
    }

    @Test
    public void showPrimaryIndexStats() throws Exception {
        JobSpecification spec = new JobSpecification();

        TreeIndexStatsOperatorDescriptor primaryStatsOp = new TreeIndexStatsOperatorDescriptor(spec, storageManager,
                lcManagerProvider, primarySplitProvider, primaryTypeTraits, primaryComparatorFactories,
                primaryBloomFilterKeyFields, dataflowHelperFactory, NoOpOperationCallbackFactory.INSTANCE);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryStatsOp, NC1_ID);
        IFileSplitProvider outSplits = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC1_ID,
                createTempFile().getAbsolutePath()) });
        IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outSplits, ",");
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        spec.connect(new OneToOneConnectorDescriptor(spec), primaryStatsOp, 0, printer, 0);
        spec.addRoot(printer);
        runTest(spec);
    }

    @Override
    protected IIndexDataflowHelperFactory createDataFlowHelperFactory() {
        return ((BTreeOperatorTestHelper) testHelper).createDataFlowHelperFactory();
    }

    @Override
    public void cleanup() throws Exception {
        destroyPrimaryIndex();
    }
}
