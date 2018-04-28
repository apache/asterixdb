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
package org.apache.asterix.test.ioopcallbacks;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.FlushOperation;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentId;
import org.mockito.Mockito;

public class TestFlushOperation extends FlushOperation {

    private final LSMComponentFileReferences files;
    private final ILSMMemoryComponent flushingComponent;

    public TestFlushOperation(ILSMIndexAccessor accessor, FileReference target, ILSMIOOperationCallback callback,
            String indexIdentifier, LSMComponentFileReferences files, LSMComponentId componentId)
            throws HyracksDataException {
        super(accessor, target, callback, indexIdentifier);
        this.files = files;
        flushingComponent = accessor.getOpContext().getIndex().getCurrentMemoryComponent();
        Mockito.when(flushingComponent.getId()).thenReturn(componentId);
    }

    @Override
    public LSMComponentFileReferences getComponentFiles() {
        return files;
    }

    @Override
    public ILSMComponent getFlushingComponent() {
        return flushingComponent;
    }

}
