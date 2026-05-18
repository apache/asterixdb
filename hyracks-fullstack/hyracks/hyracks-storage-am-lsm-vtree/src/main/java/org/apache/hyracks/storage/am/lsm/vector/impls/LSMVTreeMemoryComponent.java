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

package org.apache.hyracks.storage.am.lsm.vector.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.vector.impls.VTree;

/**
 * LSM Vector Clustering Tree memory component wrapper.
 * 
 * This class wraps a VTree for use as an in-memory component
 * in the LSM framework, providing the necessary lifecycle management and
 * component filter support.
 */
public class LSMVTreeMemoryComponent extends AbstractLSMMemoryComponent {

    private final VTree vtree;
    private final LSMVTree lsmTree;

    public LSMVTreeMemoryComponent(LSMVTree lsmTree, VTree vtree, IVirtualBufferCache vbc, ILSMComponentFilter filter) {
        super(lsmTree, vbc, filter);
        this.lsmTree = lsmTree;
        this.vtree = vtree;
    }

    @Override
    public VTree getIndex() {
        return vtree;
    }

    @Override
    public void validate() throws HyracksDataException {
        vtree.validate();
    }

    @Override
    public LSMComponentFileReferences getComponentFileRefs() {
        return new LSMComponentFileReferences(vtree.getFileReference(), null, null);
    }

    @Override
    public void cleanup() throws HyracksDataException {
        // Reset VTree initialization state before cleanup so that
        // static structure directory pages are re-created when the
        // recycled component is next allocated for writes.
        vtree.resetInitialization();
        super.cleanup();
    }

    @Override
    protected void doAllocate() throws HyracksDataException {
        super.doAllocate();
        // After re-creating the index, re-initialize the static structure
        // directory pages from the shared static structure disk component.
        lsmTree.reinitializeMemoryComponent(this);
    }

    /**
     * Gets the vector dimensions from the underlying vector clustering tree.
     */
    public int getVectorDimensions() {
        return lsmTree.getVectorDimensions();
    }
}
