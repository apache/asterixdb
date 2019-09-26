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

import java.util.Map;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;

public class LoadOperation extends AbstractIoOperation {

    private final LSMComponentFileReferences fileReferences;
    private final Map<String, Object> parameters;

    public LoadOperation(LSMComponentFileReferences fileReferences, ILSMIOOperationCallback callback,
            String indexIdentifier, Map<String, Object> parameters) {
        super(null, fileReferences.getInsertIndexFileReference(), callback, indexIdentifier);
        this.fileReferences = fileReferences;
        this.parameters = parameters;
    }

    @Override
    public final LSMIOOperationType getIOOpertionType() {
        return LSMIOOperationType.LOAD;
    }

    @Override
    public LSMIOOperationStatus call() throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public LSMComponentFileReferences getComponentFiles() {
        return fileReferences;
    }

    @Override
    public void sync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Object> getParameters() {
        return parameters;
    }

    @Override
    public long getRemainingPages() {
        return 0;
    }
}
