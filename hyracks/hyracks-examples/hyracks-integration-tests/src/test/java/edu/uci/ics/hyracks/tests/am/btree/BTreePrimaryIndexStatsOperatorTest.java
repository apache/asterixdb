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

package edu.uci.ics.hyracks.tests.am.btree;

import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexStatsOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;

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
