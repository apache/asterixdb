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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;

public abstract class AbstractDiskLSMComponent extends AbstractLSMComponent {

    public AbstractDiskLSMComponent(ILSMComponentFilter filter) {
        super(filter);
        state = ComponentState.READABLE_UNWRITABLE;
    }

    public AbstractDiskLSMComponent() {
        this(null);
    }

    @Override
    public boolean threadEnter(LSMOperationType opType, boolean isMutableComponent) {
        if (state == ComponentState.INACTIVE) {
            throw new IllegalStateException("Trying to enter an inactive disk component");
        }

        switch (opType) {
            case FORCE_MODIFICATION:
            case MODIFICATION:
            case REPLICATE:
            case SEARCH:
                readerCount++;
                break;
            case MERGE:
                if (state == ComponentState.READABLE_MERGING) {
                    // This should never happen unless there are two concurrent merges that were scheduled
                    // concurrently and they have interleaving components to be merged.
                    // This should be handled properly by the merge policy, but we guard against that here anyway.
                    return false;
                }
                state = ComponentState.READABLE_MERGING;
                readerCount++;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operation " + opType);
        }
        return true;
    }

    @Override
    public void threadExit(LSMOperationType opType, boolean failedOperation, boolean isMutableComponent)
            throws HyracksDataException {
        switch (opType) {
            case MERGE:
                // In case two merge operations were scheduled to merge an overlapping set of components, the second merge will fail and it must
                // reset those components back to their previous state.
                if (failedOperation) {
                    state = ComponentState.READABLE_UNWRITABLE;
                }
            case FORCE_MODIFICATION:
            case MODIFICATION:
            case REPLICATE:
            case SEARCH:
                readerCount--;
                if (readerCount == 0 && state == ComponentState.READABLE_MERGING) {
                    state = ComponentState.INACTIVE;
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operation " + opType);
        }

        if (readerCount <= -1) {
            throw new IllegalStateException("Invalid LSM disk component readerCount: " + readerCount);
        }
    }

    @Override
    public LSMComponentType getType() {
        return LSMComponentType.DISK;
    }

    @Override
    public ComponentState getState() {
        return state;
    }

    protected abstract void destroy() throws HyracksDataException;

    public abstract long getComponentSize();

    public abstract int getFileReferenceCount();

}
