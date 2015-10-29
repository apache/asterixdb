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
package org.apache.asterix.metadata.external;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.INullWriterFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.ExternalBTreeDataflowHelper;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.ExternalBTreeDataflowHelperFactory;
import org.apache.hyracks.storage.common.IStorageManagerInterface;

/*
 * This operator is intended for using record ids to access data in external sources
 */
public class ExternalLoopkupOperatorDiscriptor extends AbstractTreeIndexOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private final IControlledAdapterFactory adapterFactory;
    private final INullWriterFactory iNullWriterFactory;

    public ExternalLoopkupOperatorDiscriptor(IOperatorDescriptorRegistry spec,
            IControlledAdapterFactory adapterFactory, RecordDescriptor outRecDesc,
            ExternalBTreeDataflowHelperFactory externalFilesIndexDataFlowHelperFactory, boolean propagateInput,
            IIndexLifecycleManagerProvider lcManagerProvider, IStorageManagerInterface storageManager,
            IFileSplitProvider fileSplitProvider, int datasetId, double bloomFilterFalsePositiveRate,
            ISearchOperationCallbackFactory searchOpCallbackFactory, boolean retainNull,
            INullWriterFactory iNullWriterFactory) {
        super(spec, 1, 1, outRecDesc, storageManager, lcManagerProvider, fileSplitProvider,
                new FilesIndexDescription().EXTERNAL_FILE_INDEX_TYPE_TRAITS,
                new FilesIndexDescription().FILES_INDEX_COMP_FACTORIES, FilesIndexDescription.BLOOM_FILTER_FIELDS,
                externalFilesIndexDataFlowHelperFactory, null, propagateInput, retainNull, iNullWriterFactory, null,
                searchOpCallbackFactory, null);
        this.adapterFactory = adapterFactory;
        this.iNullWriterFactory = iNullWriterFactory;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
            throws HyracksDataException {
        // Create a file index accessor to be used for files lookup operations
        // Note that all file index accessors will use partition 0 since we only have 1 files index per NC 
        final ExternalFileIndexAccessor fileIndexAccessor = new ExternalFileIndexAccessor(
                (ExternalBTreeDataflowHelper) dataflowHelperFactory.createIndexDataflowHelper(this, ctx, partition),
                this);
        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            // The adapter that uses the file index along with the coming tuples to access files in HDFS
            private final IControlledAdapter adapter = adapterFactory.createAdapter(ctx, fileIndexAccessor,
                    recordDescProvider.getInputRecordDescriptor(getActivityId(), 0));

            @Override
            public void open() throws HyracksDataException {
                //Open the file index accessor here
                fileIndexAccessor.openIndex();
                try {
                    adapter.initialize(ctx, iNullWriterFactory);
                } catch (Exception e) {
                    // close the files index
                    fileIndexAccessor.closeIndex();
                    throw new HyracksDataException("error during opening a controlled adapter", e);
                }
                writer.open();
            }

            @Override
            public void close() throws HyracksDataException {
                try {
                    adapter.close(writer);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new HyracksDataException("controlled adapter failed to close", e);
                } finally {
                    //close the file index
                    fileIndexAccessor.closeIndex();
                    writer.close();
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                try {
                    adapter.fail();
                    writer.fail();
                } catch (Exception e) {
                    throw new HyracksDataException("controlled adapter failed to clean up", e);
                } finally {
                    // close the open index
                    fileIndexAccessor.closeIndex();
                }
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                try {
                    adapter.nextFrame(buffer, writer);
                } catch (Exception e) {
                    throw new HyracksDataException("controlled adapter failed to process a frame", e);
                }
            }

        };
    }
}
