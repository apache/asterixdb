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
package org.apache.asterix.tools.external.data;

import java.io.IOException;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.indexing.dataflow.FileIndexTupleTranslator;
import org.apache.asterix.metadata.entities.ExternalFile;
import org.apache.asterix.metadata.external.FilesIndexDescription;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorNodePushable;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.common.api.IIndex;
import org.apache.hyracks.storage.am.common.api.IIndexBulkLoader;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.btree.impls.ExternalBTree;
import org.apache.hyracks.storage.am.lsm.btree.impls.ExternalBTree.LSMTwoPCBTreeBulkLoader;
import org.apache.hyracks.storage.common.IStorageManagerInterface;
import org.apache.hyracks.storage.common.file.ILocalResourceFactoryProvider;

/**
 * This operator is intended solely for external dataset files replicated index.
 * It either create and bulkload when used for a new index
 * or bulkmodify the index creating a hidden transaction component which later might be committed or deleted by another operator
 *
 * @author alamouda
 */
public class ExternalFilesIndexOperatorDescriptor extends AbstractTreeIndexOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private boolean createNewIndex;
    private List<ExternalFile> files;

    public ExternalFilesIndexOperatorDescriptor(IOperatorDescriptorRegistry spec,
            IStorageManagerInterface storageManager, IIndexLifecycleManagerProvider lifecycleManagerProvider,
            IFileSplitProvider fileSplitProvider, IIndexDataflowHelperFactory dataflowHelperFactory,
            ILocalResourceFactoryProvider localResourceFactoryProvider, List<ExternalFile> files, boolean createNewIndex) {
        super(spec, 0, 0, null, storageManager, lifecycleManagerProvider, fileSplitProvider,
                FilesIndexDescription.EXTERNAL_FILE_INDEX_TYPE_TRAITS,
                FilesIndexDescription.FILES_INDEX_COMP_FACTORIES, FilesIndexDescription.BLOOM_FILTER_FIELDS,
                dataflowHelperFactory, null, false, false, null, localResourceFactoryProvider,
                NoOpOperationCallbackFactory.INSTANCE, NoOpOperationCallbackFactory.INSTANCE);
        this.createNewIndex = createNewIndex;
        this.files = files;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        final IIndexDataflowHelper indexHelper = getIndexDataflowHelperFactory().createIndexDataflowHelper(this, ctx,
                partition);
        return new AbstractOperatorNodePushable() {

            @SuppressWarnings("incomplete-switch")
            @Override
            public void initialize() throws HyracksDataException {
                FileIndexTupleTranslator filesTupleTranslator = new FileIndexTupleTranslator();
                if (createNewIndex) {
                    // Create
                    indexHelper.create();
                    // Open and get
                    indexHelper.open();
                    try {
                        IIndex index = indexHelper.getIndexInstance();
                        // Create bulk loader

                        IIndexBulkLoader bulkLoader = index.createBulkLoader(BTree.DEFAULT_FILL_FACTOR, false,
                                files.size(), false);
                        // Load files
                        for (ExternalFile file : files) {
                            bulkLoader.add(filesTupleTranslator.getTupleFromFile(file));
                        }
                        bulkLoader.end();
                    } catch (IndexException | IOException | AsterixException e) {
                        throw new HyracksDataException(e);
                    } finally {
                        indexHelper.close();
                    }
                } else {
                    ///////// Bulk modify //////////
                    // Open and get
                    indexHelper.open();
                    IIndex index = indexHelper.getIndexInstance();
                    LSMTwoPCBTreeBulkLoader bulkLoader = null;
                    try {
                        bulkLoader = (LSMTwoPCBTreeBulkLoader) ((ExternalBTree) index).createTransactionBulkLoader(
                                BTree.DEFAULT_FILL_FACTOR, false, files.size(), false);
                        // Load files
                        // The files must be ordered according to their numbers
                        for (ExternalFile file : files) {
                            switch (file.getPendingOp()) {
                                case PENDING_ADD_OP:
                                case PENDING_APPEND_OP:
                                    bulkLoader.add(filesTupleTranslator.getTupleFromFile(file));
                                    break;
                                case PENDING_DROP_OP:
                                    bulkLoader.delete(filesTupleTranslator.getTupleFromFile(file));
                                    break;
                            }
                        }
                        bulkLoader.end();
                    } catch (IndexException | IOException | AsterixException e) {
                        if (bulkLoader != null) {
                            bulkLoader.abort();
                        }
                        throw new HyracksDataException(e);
                    } finally {
                        indexHelper.close();
                    }
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
