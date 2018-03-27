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
package org.apache.asterix.external.operators;

import java.nio.ByteBuffer;

import org.apache.asterix.external.adapter.factory.LookupAdapterFactory;
import org.apache.asterix.external.dataset.adapter.LookupAdapter;
import org.apache.asterix.external.indexing.ExternalFileIndexAccessor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;

/*
 * This operator is intended for using record ids to access data in external sources
 */
public class ExternalLookupOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private final LookupAdapterFactory<?> adapterFactory;
    private final IIndexDataflowHelperFactory dataflowHelperFactory;
    private final int version;
    private final ISearchOperationCallbackFactory searchOpCallbackFactory;

    public ExternalLookupOperatorDescriptor(IOperatorDescriptorRegistry spec, LookupAdapterFactory<?> adapterFactory,
            RecordDescriptor outRecDesc, IIndexDataflowHelperFactory dataflowHelperFactory,
            ISearchOperationCallbackFactory searchOpCallbackFactory, int version) {
        super(spec, 1, 1);
        outRecDescs[0] = outRecDesc;
        this.dataflowHelperFactory = dataflowHelperFactory;
        this.searchOpCallbackFactory = searchOpCallbackFactory;
        this.version = version;
        this.adapterFactory = adapterFactory;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
            throws HyracksDataException {
        // Create a file index accessor to be used for files lookup operations
        final ExternalFileIndexAccessor snapshotAccessor = new ExternalFileIndexAccessor(
                dataflowHelperFactory.create(ctx.getJobletContext().getServiceContext(), partition),
                searchOpCallbackFactory, version);
        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            // The adapter that uses the file index along with the coming tuples to access files in HDFS
            private LookupAdapter<?> adapter;
            private boolean indexOpen = false;

            @Override
            public void open() throws HyracksDataException {
                try {
                    adapter = adapterFactory.createAdapter(ctx, partition,
                            recordDescProvider.getInputRecordDescriptor(getActivityId(), 0), snapshotAccessor, writer);
                    // Open the file index accessor here
                    snapshotAccessor.open();
                    indexOpen = true;
                    adapter.open();
                } catch (Throwable th) {
                    throw HyracksDataException.create(th);
                }
            }

            @Override
            public void close() throws HyracksDataException {
                HyracksDataException hde = null;
                if (indexOpen) {
                    try {
                        snapshotAccessor.close();
                    } catch (Throwable th) {
                        hde = HyracksDataException.create(th);
                    }
                    try {
                        adapter.close();
                    } catch (Throwable th) {
                        if (hde == null) {
                            hde = HyracksDataException.create(th);
                        } else {
                            hde.addSuppressed(th);
                        }
                    }
                }
                if (hde != null) {
                    throw hde;
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                try {
                    adapter.fail();
                } catch (Throwable th) {
                    throw HyracksDataException.create(th);
                }
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                try {
                    adapter.nextFrame(buffer);
                } catch (Throwable th) {
                    throw HyracksDataException.create(th);
                }
            }

            @Override
            public void flush() throws HyracksDataException {
                adapter.flush();
            }
        };
    }
}
