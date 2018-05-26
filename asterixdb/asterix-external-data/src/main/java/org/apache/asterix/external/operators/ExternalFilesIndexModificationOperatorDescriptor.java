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

import org.apache.asterix.common.exceptions.ErrorCode;
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
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.btree.impls.ExternalBTree;
import org.apache.hyracks.storage.am.lsm.btree.impls.ExternalBTree.LSMTwoPCBTreeBulkLoader;
import org.apache.hyracks.storage.common.IIndex;

/**
 * This operator is intended solely for external dataset files replicated index.
 * It bulkmodify the index creating a hidden transaction component which later might be committed or deleted
 */
public class ExternalFilesIndexModificationOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private List<ExternalFile> files;
    private IIndexDataflowHelperFactory dataflowHelperFactory;

    public ExternalFilesIndexModificationOperatorDescriptor(IOperatorDescriptorRegistry spec,
            IIndexDataflowHelperFactory dataflowHelperFactory, List<ExternalFile> files) {
        super(spec, 0, 0);
        this.dataflowHelperFactory = dataflowHelperFactory;
        this.files = files;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new AbstractOperatorNodePushable() {
            @Override
            public void initialize() throws HyracksDataException {
                final IIndexDataflowHelper indexHelper =
                        dataflowHelperFactory.create(ctx.getJobletContext().getServiceContext(), partition);
                FileIndexTupleTranslator filesTupleTranslator = new FileIndexTupleTranslator();
                // Open and get
                indexHelper.open();
                IIndex index = indexHelper.getIndexInstance();
                LSMTwoPCBTreeBulkLoader bulkLoader = null;
                Map<String, Object> parameters = new HashMap<>();
                try {
                    bulkLoader = (LSMTwoPCBTreeBulkLoader) ((ExternalBTree) index)
                            .createTransactionBulkLoader(BTree.DEFAULT_FILL_FACTOR, false, files.size(), parameters);
                    // Load files
                    // The files must be ordered according to their numbers
                    for (ExternalFile file : files) {
                        switch (file.getPendingOp()) {
                            case ADD_OP:
                            case APPEND_OP:
                                bulkLoader.add(filesTupleTranslator.getTupleFromFile(file));
                                break;
                            case DROP_OP:
                                bulkLoader.delete(filesTupleTranslator.getTupleFromFile(file));
                                break;
                            case NO_OP:
                                break;
                            default:
                                throw HyracksDataException.create(ErrorCode.UNKNOWN_EXTERNAL_FILE_PENDING_OP, sourceLoc,
                                        file.getPendingOp());
                        }
                    }
                    bulkLoader.end();
                } catch (Exception e) {
                    if (bulkLoader != null) {
                        bulkLoader.abort();
                    }
                    throw HyracksDataException.create(e);
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
