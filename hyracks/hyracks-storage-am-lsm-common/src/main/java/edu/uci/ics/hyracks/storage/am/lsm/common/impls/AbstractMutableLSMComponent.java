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
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IMutableComponentAdderCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IMutableComponentSwitcherCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;

public abstract class AbstractMutableLSMComponent implements ILSMComponent {

    private int readerCount;
    private int writerCount;
    private ComponentState state;
    private final IVirtualBufferCache vbc;

    private IMutableComponentAdderCallback adderCallback;
    private IMutableComponentSwitcherCallback switcherCallback;

    private final AtomicBoolean isModified;
    private boolean requestedToBeActive;

    private enum ComponentState {
        READABLE_WRITABLE,
        READABLE_UNWRITABLE,
        READABLE_UNWRITABLE_FLUSHING,
        UNREADABLE_UNWRITABLE,
        INACTIVE_READABLE_WRITABLE
    }

    public AbstractMutableLSMComponent(IVirtualBufferCache vbc, boolean isActive) {
        this.vbc = vbc;
        readerCount = 0;
        writerCount = 0;
        if (isActive) {
            state = ComponentState.READABLE_WRITABLE;
        } else {
            state = ComponentState.INACTIVE_READABLE_WRITABLE;
        }
        isModified = new AtomicBoolean();
    }

    @Override
    public boolean threadEnter(LSMOperationType opType, boolean firstComponent) throws InterruptedException,
            HyracksDataException {
        if (state == ComponentState.INACTIVE_READABLE_WRITABLE && requestedToBeActive) {
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
                if (state == ComponentState.UNREADABLE_UNWRITABLE) {
                    return false;
                }
                readerCount++;
                break;
            case FLUSH:
                if (state == ComponentState.READABLE_UNWRITABLE_FLUSHING
                        || state == ComponentState.UNREADABLE_UNWRITABLE
                        || state == ComponentState.INACTIVE_READABLE_WRITABLE) {
                    return false;
                }

                state = ComponentState.READABLE_UNWRITABLE_FLUSHING;
                switcherCallback.requestFlush(false);
                synchronized (this) {
                    while (writerCount > 0) {
                        wait();
                    }
                }
                switcherCallback.switchComponents();
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
                        adderCallback.addComponent();
                        state = ComponentState.INACTIVE_READABLE_WRITABLE;
                    }
                }
                break;
            case SEARCH:
                readerCount--;
                if (state == ComponentState.UNREADABLE_UNWRITABLE && readerCount == 0) {
                    reset();
                    adderCallback.addComponent();
                    state = ComponentState.INACTIVE_READABLE_WRITABLE;
                }
                break;
            case FLUSH:
                assert state == ComponentState.READABLE_UNWRITABLE_FLUSHING;
                readerCount--;
                if (readerCount == 0) {
                    reset();
                    adderCallback.addComponent();
                    state = ComponentState.INACTIVE_READABLE_WRITABLE;
                } else {
                    state = ComponentState.UNREADABLE_UNWRITABLE;
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operation " + opType);
        }
        synchronized (this) {
            notifyAll();
        }
    }

    public boolean isReadable() {
        if (state == ComponentState.INACTIVE_READABLE_WRITABLE || state == ComponentState.UNREADABLE_UNWRITABLE) {
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

    public void registerOnResetCallback(IMutableComponentAdderCallback adderCallback) {
        this.adderCallback = adderCallback;
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
