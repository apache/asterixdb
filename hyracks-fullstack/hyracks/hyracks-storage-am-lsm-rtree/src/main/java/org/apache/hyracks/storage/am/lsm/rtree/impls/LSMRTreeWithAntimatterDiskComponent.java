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
package org.apache.hyracks.storage.am.lsm.rtree.impls;

import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.rtree.impls.RTree;

public class LSMRTreeWithAntimatterDiskComponent extends AbstractLSMDiskComponent {
    private final RTree rtree;

    public LSMRTreeWithAntimatterDiskComponent(AbstractLSMIndex lsmIndex, RTree rtree, ILSMComponentFilter filter) {
        super(lsmIndex, LSMRTreeDiskComponent.getMetadataPageManager(rtree), filter);
        this.rtree = rtree;
    }

    @Override
    public RTree getIndex() {
        return rtree;
    }

    @Override
    public RTree getMetadataHolder() {
        return rtree;
    }

    @Override
    public long getComponentSize() {
        return LSMRTreeDiskComponent.getComponentSize(rtree);
    }

    @Override
    public int getFileReferenceCount() {
        return LSMRTreeDiskComponent.getFileReferenceCount(rtree);
    }

    @Override
    public Set<String> getLSMComponentPhysicalFiles() {
        return LSMRTreeDiskComponent.getFiles(rtree);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + rtree.getFileReference().getRelativePath();
    }

    @Override
    public void validate() throws HyracksDataException {
        throw new UnsupportedOperationException("Validation not implemented for LSM R-Trees.");
    }
}
