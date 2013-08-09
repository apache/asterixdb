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
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;

public abstract class AbstractImmutableLSMComponent implements ILSMComponent {

    private ComponentState state;
    private int readerCount;

    private enum ComponentState {
        READABLE,
        READABLE_MERGING,
        KILLED
    }

    public AbstractImmutableLSMComponent() {
        state = ComponentState.READABLE;
        readerCount = 0;
    }

    @Override
    public boolean threadEnter(LSMOperationType opType, boolean firstComponent) {
        if (state == ComponentState.KILLED) {
            return false;
        }

        switch (opType) {
            case FORCE_MODIFICATION:
            case MODIFICATION:
            case SEARCH:
                readerCount++;
                break;
            case MERGE:
                if (state == ComponentState.READABLE_MERGING) {
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
    public void threadExit(LSMOperationType opType, boolean failedOperation, boolean firstComponent)
            throws HyracksDataException {
        switch (opType) {
            case MERGE:
                if (failedOperation) {
                    state = ComponentState.READABLE;
                }
            case FORCE_MODIFICATION:
            case MODIFICATION:
            case SEARCH:
                readerCount--;

                if (readerCount == 0 && state == ComponentState.READABLE_MERGING) {
                    destroy();
                    state = ComponentState.KILLED;
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operation " + opType);
        }
    }

    @Override
    public LSMComponentType getType() {
        return LSMComponentType.DISK;
    }

    protected abstract void destroy() throws HyracksDataException;

}
