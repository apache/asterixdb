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

import java.util.Objects;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;

public abstract class FlushOperation extends AbstractIoOperation {

    public FlushOperation(ILSMIndexAccessor accessor, FileReference target, ILSMIOOperationCallback callback,
            String indexIdentifier) {
        super(accessor, target, callback, indexIdentifier);
    }

    @Override
    public LSMIOOperationStatus call() throws HyracksDataException {
        accessor.flush(this);
        return getStatus();
    }

    @Override
    public ILSMIOOperationCallback getCallback() {
        return callback;
    }

    @Override
    public ILSMIndexAccessor getAccessor() {
        return accessor;
    }

    public ILSMComponent getFlushingComponent() {
        return accessor.getOpContext().getComponentHolder().get(0);
    }

    @Override
    public String getIndexIdentifier() {
        return indexIdentifier;
    }

    @Override
    public LSMIOOperationType getIOOpertionType() {
        return LSMIOOperationType.FLUSH;
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof FlushOperation)
                && Objects.equals(target.getFile().getName(), ((FlushOperation) o).target.getFile().getName());
    }

    @Override
    public int hashCode() {
        return target.getFile().getName().hashCode();
    }

    @Override
    public long getRemainingPages() {
        return 0;
    }
}
