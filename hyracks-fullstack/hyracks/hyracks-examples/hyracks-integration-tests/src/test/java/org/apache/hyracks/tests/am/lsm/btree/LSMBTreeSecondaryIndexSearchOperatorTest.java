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

package org.apache.hyracks.tests.am.lsm.btree;

import java.io.DataOutput;

import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;
import org.apache.hyracks.dataflow.std.misc.ConstantTupleSourceOperatorDescriptor;
import org.apache.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.common.IResourceFactory;
import org.apache.hyracks.test.support.TestStorageManagerComponentHolder;
import org.apache.hyracks.tests.am.btree.BTreeSecondaryIndexSearchOperatorTest;
import org.apache.hyracks.tests.am.btree.DataSetConstants;
import org.apache.hyracks.tests.am.common.ITreeIndexOperatorTestHelper;
import org.apache.hyracks.tests.util.NoopMissingWriterFactory;
import org.junit.Test;

public class LSMBTreeSecondaryIndexSearchOperatorTest extends BTreeSecondaryIndexSearchOperatorTest {
    @Override
    protected ITreeIndexOperatorTestHelper createTestHelper() throws HyracksDataException {
        return new LSMBTreeOperatorTestHelper(TestStorageManagerComponentHolder.getIOManager());
    }

    @Override
    protected IResourceFactory createPrimaryResourceFactory() {
        return ((LSMBTreeOperatorTestHelper) testHelper).getLocalResourceFactory(storageManager,
                DataSetConstants.primaryTypeTraits, DataSetConstants.primaryComparatorFactories,
                (IMetadataPageManagerFactory) pageManagerFactory, DataSetConstants.primaryBloomFilterKeyFields,
                DataSetConstants.primaryBtreeFields, DataSetConstants.primaryFilterFields,
                DataSetConstants.filterTypeTraits, DataSetConstants.filterCmpFactories);
    }

    @Test
    public void shouldCarryFilterValueToPrimaryIndexSearch() throws Exception {
        JobSpecification spec = new JobSpecification();

        // build tuple containing search keys (only use the first key as search
        // key)
        ArrayTupleBuilder tb = new ArrayTupleBuilder(DataSetConstants.secondaryKeyFieldCount);
        DataOutput dos = tb.getDataOutput();

        tb.reset();
        // low key
        new UTF8StringSerializerDeserializer().serialize("1998-07-21", dos);
        tb.addFieldEndOffset();
        // high key
        new UTF8StringSerializerDeserializer().serialize("2000-10-18", dos);
        tb.addFieldEndOffset();

        ISerializerDeserializer[] keyRecDescSers =
                { new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() };
        RecordDescriptor keyRecDesc = new RecordDescriptor(keyRecDescSers);

        ConstantTupleSourceOperatorDescriptor keyProviderOp = new ConstantTupleSourceOperatorDescriptor(spec,
                keyRecDesc, tb.getFieldEndOffsets(), tb.getByteArray(), tb.getSize());
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, keyProviderOp, NC1_ID);

        int[] secondaryLowKeyFields = { 0 };
        int[] secondaryHighKeyFields = { 1 };

        // search secondary index
        BTreeSearchOperatorDescriptor secondaryBtreeSearchOp =
                new BTreeSearchOperatorDescriptor(spec, DataSetConstants.secondaryWithFilterRecDesc,
                        secondaryLowKeyFields, secondaryHighKeyFields, true, true, secondaryHelperFactory, false, false,
                        NoopMissingWriterFactory.INSTANCE, NoOpOperationCallbackFactory.INSTANCE, null, null, true);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, secondaryBtreeSearchOp, NC1_ID);

        int[] primaryLowKeyFields = { 1 }; // second field from the tuples
        int[] primaryHighKeyFields = { 1 }; // second field from the tuples
        int[] minFilterFields = { 2 };
        int[] maxFilterFields = { 3 };

        // search primary index
        BTreeSearchOperatorDescriptor primaryBtreeSearchOp = new BTreeSearchOperatorDescriptor(spec,
                DataSetConstants.primaryRecDesc, primaryLowKeyFields, primaryHighKeyFields, true, true,
                primaryHelperFactory, false, false, NoopMissingWriterFactory.INSTANCE,
                NoOpOperationCallbackFactory.INSTANCE, minFilterFields, maxFilterFields, false);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryBtreeSearchOp, NC1_ID);
        IFileSplitProvider outSplits = new ConstantFileSplitProvider(new FileSplit[] { createFile(nc1) });
        IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outSplits, ",");
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);
        spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, secondaryBtreeSearchOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), secondaryBtreeSearchOp, 0, primaryBtreeSearchOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), primaryBtreeSearchOp, 0, printer, 0);
        spec.addRoot(printer);
        runTest(spec);
    }

    @Override
    protected IResourceFactory createSecondaryResourceFactory() {
        return ((LSMBTreeOperatorTestHelper) testHelper).getLocalResourceFactory(storageManager,
                DataSetConstants.secondaryTypeTraits, DataSetConstants.secondaryComparatorFactories,
                (IMetadataPageManagerFactory) pageManagerFactory, DataSetConstants.secondaryBloomFilterKeyFields,
                DataSetConstants.secondaryBtreeFields, DataSetConstants.secondaryFilterFields,
                DataSetConstants.filterTypeTraits, DataSetConstants.filterCmpFactories);
    }

}
