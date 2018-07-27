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

package org.apache.hyracks.storage.am.lsm.common.impls;

import org.apache.hyracks.api.io.FileReference;

public final class LSMComponentFileReferences {

    // The FileReference for the index that is used for inserting records of the component. For instance, this will be the FileReference of the RTree in one component of the LSM-RTree.
    private final FileReference insertIndexFileReference;
    // This FileReference for the delete index (if any). For example, this will be the the FileReference of the buddy BTree in one component of the LSM-RTree.
    private final FileReference deleteIndexFileReference;

    // This FileReference for the bloom filter (if any).
    private final FileReference bloomFilterFileReference;

    public LSMComponentFileReferences(FileReference insertIndexFileReference, FileReference deleteIndexFileReference,
            FileReference bloomFilterFileReference) {
        this.insertIndexFileReference = insertIndexFileReference;
        this.deleteIndexFileReference = deleteIndexFileReference;
        this.bloomFilterFileReference = bloomFilterFileReference;
    }

    public FileReference getInsertIndexFileReference() {
        return insertIndexFileReference;
    }

    public FileReference getDeleteIndexFileReference() {
        return deleteIndexFileReference;
    }

    public FileReference getBloomFilterFileReference() {
        return bloomFilterFileReference;
    }

    public FileReference[] getFileReferences() {
        return new FileReference[] { insertIndexFileReference, deleteIndexFileReference, bloomFilterFileReference };
    }

    @Override
    public String toString() {
        return "{ \"insert\" : \"" + insertIndexFileReference + "\", \"delete\" : \"" + deleteIndexFileReference
                + "\", \"bloom\" : \"" + bloomFilterFileReference + "\"}";
    }
}
