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

import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.impls.FlushOperation;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;

/**
 * Flush operation for LSM Vector Clustering Tree.
 * This operation flushes a memory component to disk, creating a new disk component.
 */
public class LSMVTreeFlushOperation extends FlushOperation {
    private final LSMComponentFileReferences fileReferences;

    public LSMVTreeFlushOperation(ILSMIndexAccessor accessor, FileReference flushTarget,
            ILSMIOOperationCallback callback, String indexIdentifier) {
        super(accessor, flushTarget, callback, indexIdentifier);
        // Vector clustering tree only has a single primary file (no bloom filter or secondary indexes)
        fileReferences = new LSMComponentFileReferences(target, null, null);
    }

    @Override
    public LSMComponentFileReferences getComponentFiles() {
        return fileReferences;
    }
}
