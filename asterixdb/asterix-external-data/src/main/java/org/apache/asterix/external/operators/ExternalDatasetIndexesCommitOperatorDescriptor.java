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

import java.util.List;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.storage.am.common.api.IIndex;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.util.IndexFileNameUtil;
import org.apache.hyracks.storage.am.lsm.common.api.ITwoPCIndex;
import org.apache.log4j.Logger;

public class ExternalDatasetIndexesCommitOperatorDescriptor extends AbstractExternalDatasetIndexesOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER =
            Logger.getLogger(ExternalDatasetIndexesCommitOperatorDescriptor.class.getName());

    public ExternalDatasetIndexesCommitOperatorDescriptor(IOperatorDescriptorRegistry spec,
            IIndexDataflowHelperFactory filesIndexDataflowHelperFactory, IndexInfoOperatorDescriptor fileIndexesInfo,
            List<IIndexDataflowHelperFactory> indexesDataflowHelperFactories,
            List<IndexInfoOperatorDescriptor> indexesInfos) {
        super(spec, filesIndexDataflowHelperFactory, fileIndexesInfo, indexesDataflowHelperFactories,
                indexesInfos);
    }

    @Override
    protected void performOpOnIndex(IIndexDataflowHelperFactory indexDataflowHelperFactory, IHyracksTaskContext ctx,
            IndexInfoOperatorDescriptor fileIndexInfo, int partition) {
        try {
            FileReference resourecePath =
                    IndexFileNameUtil.getIndexAbsoluteFileRef(fileIndexInfo, partition, ctx.getIOManager());
            LOGGER.warn("performing the operation on " + resourecePath.getFile().getAbsolutePath());
            // Get DataflowHelper
            IIndexDataflowHelper indexHelper =
                    indexDataflowHelperFactory.createIndexDataflowHelper(fileIndexInfo, ctx, partition);
            // Get index
            IIndex index = indexHelper.getIndexInstance();
            // commit transaction
            ((ITwoPCIndex) index).commitTransaction();
            LOGGER.warn("operation on " + resourecePath.getFile().getAbsolutePath() + " Succeded");
        } catch (HyracksDataException | IndexException e) {
            throw new IllegalStateException(e);
        }
    }

}
