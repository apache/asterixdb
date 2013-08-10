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

import java.util.concurrent.atomic.AtomicBoolean;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IMutableComponentSwitcherCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;

public abstract class AbstractMutableLSMComponent implements ILSMComponent {

    private int readerCount;
    private int writerCount;
    private ComponentState state;
    private final IVirtualBufferCache vbc;

    private IMutableComponentSwitcherCallback switcherCallback;

    private final AtomicBoolean isModified;
    private boolean requestedToBeActive;

    private enum ComponentState {
        READABLE_WRITABLE,
        READABLE_UNWRITABLE,
        READABLE_UNWRITABLE_FLUSHING,
        UNREADABLE_UNWRITABLE,
        INACTIVE
    }

    public AbstractMutableLSMComponent(IVirtualBufferCache vbc, boolean isActive) {
        this.vbc = vbc;
        readerCount = 0;
        writerCount = 0;
        if (isActive) {
            state = ComponentState.READABLE_WRITABLE;
        } else {
            state = ComponentState.INACTIVE;
        }
        isModified = new AtomicBoolean();
    }

    @Override
    public boolean threadEnter(LSMOperationType opType, boolean firstComponent) throws HyracksDataException {
        if (state == ComponentState.INACTIVE && requestedToBeActive) {
            state = ComponentState.READABLE_WRITABLE;
            requestedToBeActive = false;
        }
        switch (opType) {
            case FORCE_MODIFICATION:
                if (firstComponent) {
                    if (state == ComponentState.READABLE_WRITABLE || state == ComponentState.READABLE_UNWRITABLE) {
                        writerCount++;
                    } else {
                        return false;
                    }
                } else {
                    if (state == ComponentState.READABLE_UNWRITABLE
                            || state == ComponentState.READABLE_UNWRITABLE_FLUSHING) {
                        readerCount++;
                    } else {
                        return false;
                    }
                }
                break;
            case MODIFICATION:
                if (firstComponent) {
                    if (state == ComponentState.READABLE_WRITABLE) {
                        writerCount++;
                    } else {
                        return false;
                    }
                } else {
                    if (state == ComponentState.READABLE_UNWRITABLE
                            || state == ComponentState.READABLE_UNWRITABLE_FLUSHING) {
                        readerCount++;
                    } else {
                        return false;
                    }
                }
                break;
            case SEARCH:
                if (state == ComponentState.READABLE_WRITABLE || state == ComponentState.READABLE_UNWRITABLE
                        || state == ComponentState.READABLE_UNWRITABLE_FLUSHING) {
                    readerCount++;
                } else {
                    return false;
                }
                break;
            case FLUSH:
                if (state == ComponentState.READABLE_WRITABLE || state == ComponentState.READABLE_UNWRITABLE) {
                    assert writerCount == 0;
                    state = ComponentState.READABLE_UNWRITABLE_FLUSHING;
                    switcherCallback.requestFlush(false);
                    switcherCallback.switchComponents();
                    readerCount++;
                } else {
                    return false;
                }
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
            case FORCE_MODIFICATION:
            case MODIFICATION:
                if (firstComponent) {
                    writerCount--;
                    if (state == ComponentState.READABLE_WRITABLE && isFull() && !failedOperation) {
                        state = ComponentState.READABLE_UNWRITABLE;
                        switcherCallback.requestFlush(true);
                    }
                } else {
                    readerCount--;
                    if (state == ComponentState.UNREADABLE_UNWRITABLE && readerCount == 0) {
                        reset();
                        state = ComponentState.INACTIVE;
                    }
                }
                break;
            case SEARCH:
                readerCount--;
                if (state == ComponentState.UNREADABLE_UNWRITABLE && readerCount == 0) {
                    reset();
                    state = ComponentState.INACTIVE;
                }
                break;
            case FLUSH:
                assert state == ComponentState.READABLE_UNWRITABLE_FLUSHING;
                readerCount--;
                if (readerCount == 0) {
                    reset();
                    state = ComponentState.INACTIVE;
                } else {
                    state = ComponentState.UNREADABLE_UNWRITABLE;
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operation " + opType);
        }
    }

    public boolean isReadable() {
        if (state == ComponentState.INACTIVE || state == ComponentState.UNREADABLE_UNWRITABLE) {
            return false;
        }
        return true;
    }

    @Override
    public LSMComponentType getType() {
        return LSMComponentType.MEMORY;
    }

    public void setActive() {
        requestedToBeActive = true;
    }

    public void registerOnFlushCallback(IMutableComponentSwitcherCallback switcherCallback) {
        this.switcherCallback = switcherCallback;
    }

    public void setIsModified() {
        isModified.set(true);
    }

    public boolean isModified() {
        return isModified.get();
    }

    public boolean isFull() {
        return vbc.isFull();
    }

    protected void reset() throws HyracksDataException {
        isModified.set(false);
    }
}
