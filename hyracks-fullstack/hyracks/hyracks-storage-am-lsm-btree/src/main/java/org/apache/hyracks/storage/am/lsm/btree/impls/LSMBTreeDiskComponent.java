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
package org.apache.hyracks.storage.am.lsm.btree.impls;

import java.util.HashSet;
import java.util.Set;

import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.DiskBTree;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.common.compression.file.CompressedFileReference;

public class LSMBTreeDiskComponent extends AbstractLSMDiskComponent {
    protected final DiskBTree btree;

    public LSMBTreeDiskComponent(AbstractLSMIndex lsmIndex, DiskBTree btree, ILSMComponentFilter filter) {
        super(lsmIndex, getMetadataPageManager(btree), filter);
        this.btree = btree;
    }

    @Override
    public DiskBTree getIndex() {
        return btree;
    }

    @Override
    public DiskBTree getMetadataHolder() {
        return btree;
    }

    @Override
    public long getComponentSize() {
        return getComponentSize(btree);
    }

    @Override
    public int getFileReferenceCount() {
        return getFileReferenceCount(btree);
    }

    @Override
    public Set<String> getLSMComponentPhysicalFiles() {
        return getFiles(btree);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + btree.getFileReference().getRelativePath();
    }

    static IMetadataPageManager getMetadataPageManager(BTree btree) {
        return (IMetadataPageManager) btree.getPageManager();
    }

    static long getComponentSize(BTree btree) {
        return btree.getFileReference().getFile().length();
    }

    static int getFileReferenceCount(BTree btree) {
        return btree.getBufferCache().getFileReferenceCount(btree.getFileId());
    }

    static Set<String> getFiles(BTree btree) {
        Set<String> files = new HashSet<>();
        final FileReference fileRef = btree.getFileReference();
        files.add(fileRef.getAbsolutePath());
        if (fileRef.isCompressed()) {
            final CompressedFileReference cFileRef = (CompressedFileReference) fileRef;
            files.add(cFileRef.getLAFAbsolutePath());
        }
        return files;
    }
}
