/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public abstract class AbstractDiskLSMComponent extends AbstractLSMComponent {

    public AbstractDiskLSMComponent() {
        super();
        state = ComponentState.READABLE_UNWRITABLE;
    }

    @Override
    public boolean threadEnter(LSMOperationType opType, boolean isMutableComponent) {
        assert state != ComponentState.INACTIVE;

        switch (opType) {
            case FORCE_MODIFICATION:
            case MODIFICATION:
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
            case SEARCH:
                readerCount--;
                if (readerCount == 0 && state == ComponentState.READABLE_MERGING) {
                    state = ComponentState.INACTIVE;
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operation " + opType);
        }
        assert readerCount > -1;
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

}
