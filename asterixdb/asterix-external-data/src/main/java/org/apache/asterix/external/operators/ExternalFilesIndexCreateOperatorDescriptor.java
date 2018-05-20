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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.ioopcallbacks.LSMIOOperationCallback;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.external.indexing.FileIndexTupleTranslator;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.common.api.IIndexBuilder;
import org.apache.hyracks.storage.am.common.api.IIndexBuilderFactory;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentId;
import org.apache.hyracks.storage.common.IIndexBulkLoader;

/**
 * For the replicated file index
 * It creates and bulkloads initial set of files
 */
public class ExternalFilesIndexCreateOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private List<ExternalFile> files;
    private IIndexDataflowHelperFactory dataflowHelperFactory;
    private IIndexBuilderFactory indexBuilderFactory;

    public ExternalFilesIndexCreateOperatorDescriptor(IOperatorDescriptorRegistry spec,
            IIndexBuilderFactory indexBuilderFactory, IIndexDataflowHelperFactory dataflowHelperFactory,
            List<ExternalFile> files) {
        super(spec, 0, 0);
        this.indexBuilderFactory = indexBuilderFactory;
        this.dataflowHelperFactory = dataflowHelperFactory;
        this.files = files;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new AbstractOperatorNodePushable() {
            @Override
            public void initialize() throws HyracksDataException {
                IIndexBuilder indexBuilder = indexBuilderFactory.create(ctx, partition);
                IIndexDataflowHelper indexHelper =
                        dataflowHelperFactory.create(ctx.getJobletContext().getServiceContext(), partition);
                FileIndexTupleTranslator filesTupleTranslator = new FileIndexTupleTranslator();
                // Build the index
                indexBuilder.build();
                // Open the index
                indexHelper.open();
                try {
                    ILSMIndex index = (ILSMIndex) indexHelper.getIndexInstance();
                    Map<String, Object> parameters = new HashMap<>();
                    parameters.put(LSMIOOperationCallback.KEY_FLUSHED_COMPONENT_ID,
                            LSMComponentId.DEFAULT_COMPONENT_ID);
                    // Create bulk loader
                    IIndexBulkLoader bulkLoader =
                            index.createBulkLoader(BTree.DEFAULT_FILL_FACTOR, false, files.size(), false, parameters);
                    // Load files
                    for (ExternalFile file : files) {
                        bulkLoader.add(filesTupleTranslator.getTupleFromFile(file));
                    }
                    bulkLoader.end();
                } finally {
                    indexHelper.close();
                }
            }

            @Override
            public void deinitialize() throws HyracksDataException {
            }

            @Override
            public int getInputArity() {
                return 0;
            }

            @Override
            public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc)
                    throws HyracksDataException {
            }

            @Override
            public IFrameWriter getInputFrameWriter(int index) {
                return null;
            }

        };
    }
}
