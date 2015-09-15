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
package org.apache.asterix.external.indexing.operators;

import java.io.File;
import java.util.List;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.util.IndexFileNameUtil;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.ExternalBTreeDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.ExternalBTreeWithBuddyDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.AbortRecoverLSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.rtree.dataflow.ExternalRTreeDataflowHelperFactory;

public class ExternalDatasetIndexesRecoverOperatorDescriptor extends AbstractExternalDatasetIndexesOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    public ExternalDatasetIndexesRecoverOperatorDescriptor(IOperatorDescriptorRegistry spec,
            ExternalBTreeDataflowHelperFactory filesIndexDataflowHelperFactory,
            IndexInfoOperatorDescriptor fileIndexesInfo,
            List<ExternalBTreeWithBuddyDataflowHelperFactory> bTreeIndexesDataflowHelperFactories,
            List<IndexInfoOperatorDescriptor> bTreeIndexesInfos,
            List<ExternalRTreeDataflowHelperFactory> rTreeIndexesDataflowHelperFactories,
            List<IndexInfoOperatorDescriptor> rTreeIndexesInfos) {
        super(spec, filesIndexDataflowHelperFactory, fileIndexesInfo, bTreeIndexesDataflowHelperFactories,
                bTreeIndexesInfos, rTreeIndexesDataflowHelperFactories, rTreeIndexesInfos);
    }

    @Override
    protected void performOpOnIndex(IIndexDataflowHelperFactory indexDataflowHelperFactory, IHyracksTaskContext ctx,
            IndexInfoOperatorDescriptor fileIndexInfo, int partition) throws Exception {
        FileReference file = new FileReference(new File(IndexFileNameUtil.prepareFileName(fileIndexInfo
                .getFileSplitProvider().getFileSplits()[partition].getLocalFile().getFile().getPath(), fileIndexInfo
                .getFileSplitProvider().getFileSplits()[partition].getIODeviceId())));
        AbortRecoverLSMIndexFileManager fileManager = new AbortRecoverLSMIndexFileManager(file);
        fileManager.recoverTransaction();
    }

}
