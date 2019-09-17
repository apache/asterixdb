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
package org.apache.hyracks.storage.am.lsm.common.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.common.impls.AbstractTreeIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.IChainedComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.impls.IndexWithBuddyBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.util.ComponentUtils;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.IPageWriteFailureCallback;

public abstract class AbstractLSMWithBuddyDiskComponent extends AbstractLSMWithBloomFilterDiskComponent {

    public AbstractLSMWithBuddyDiskComponent(AbstractLSMIndex lsmIndex, IMetadataPageManager mdPageManager,
            ILSMComponentFilter filter) {
        super(lsmIndex, mdPageManager, filter);
    }

    public abstract AbstractTreeIndex getBuddyIndex();

    @Override
    public void markAsValid(boolean persist, IPageWriteFailureCallback callback) throws HyracksDataException {
        super.markAsValid(persist, callback);
        ComponentUtils.markAsValid(getBuddyIndex(), persist, callback);
    }

    @Override
    public void activate(boolean createNewComponent) throws HyracksDataException {
        super.activate(createNewComponent);
        if (createNewComponent) {
            getBuddyIndex().create();
        }
        getBuddyIndex().activate();
    }

    @Override
    public void destroy() throws HyracksDataException {
        super.destroy();
        getBuddyIndex().destroy();
    }

    @Override
    public void deactivate() throws HyracksDataException {
        super.deactivate();
        getBuddyIndex().deactivate();
    }

    @Override
    protected void purge() throws HyracksDataException {
        super.purge();
        getBuddyIndex().purge();
    }

    @Override
    public void validate() throws HyracksDataException {
        super.validate();
        getBuddyIndex().validate();
    }

    @Override
    protected IChainedComponentBulkLoader createIndexBulkLoader(float fillFactor, boolean verifyInput,
            long numElementsHint, boolean checkIfEmptyIndex, IPageWriteCallback callback) throws HyracksDataException {
        IIndexBulkLoader indexBulkLoader =
                getIndex().createBulkLoader(fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex, callback);
        IIndexBulkLoader buddyBulkLoader =
                getBuddyIndex().createBulkLoader(fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex, callback);
        return new IndexWithBuddyBulkLoader(indexBulkLoader, buddyBulkLoader);
    }

}
