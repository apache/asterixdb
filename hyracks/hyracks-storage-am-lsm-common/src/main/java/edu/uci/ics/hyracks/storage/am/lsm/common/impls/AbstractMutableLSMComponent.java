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
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;

public abstract class AbstractMutableLSMComponent implements ILSMComponent {

    private int readerCount;
    private int writerCount;
    private ComponentState state;
    private IMutableResetCallback resetCallback;
    private final IVirtualBufferCache vbc;

    private final AtomicBoolean isModified;

    private enum ComponentState {
        READABLE_WRITABLE,
        READABLE_UNWRITABLE,
        READABLE_UNWRITABLE_FLUSHING,
        UNREADABLE_UNWRITABLE
    }

    public AbstractMutableLSMComponent(IVirtualBufferCache vbc) {
        this.vbc = vbc;
        readerCount = 0;
        writerCount = 0;
        state = ComponentState.READABLE_WRITABLE;
        isModified = new AtomicBoolean();
    }

    @Override
    public synchronized boolean threadEnter(LSMOperationType opType) throws InterruptedException {
        switch (opType) {
            case FORCE_MODIFICATION:
                if (state != ComponentState.READABLE_WRITABLE && state != ComponentState.READABLE_UNWRITABLE) {
                    return false;
                }
                writerCount++;
                break;
            case MODIFICATION:
                if (state != ComponentState.READABLE_WRITABLE) {
                    return false;
                }
                writerCount++;
                break;
            case SEARCH:
                if (state == ComponentState.UNREADABLE_UNWRITABLE) {
                    return false;
                }
                readerCount++;
                break;
            case FLUSH:
                if (state == ComponentState.READABLE_UNWRITABLE_FLUSHING
                        || state == ComponentState.UNREADABLE_UNWRITABLE) {
                    return false;
                }

                state = ComponentState.READABLE_UNWRITABLE_FLUSHING;
                while (writerCount > 0) {
                    wait();
                }
                readerCount++;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operation " + opType);
        }
        return true;
    }

    @Override
    public synchronized void threadExit(LSMOperationType opType, boolean failedOperation) throws HyracksDataException {
        switch (opType) {
            case FORCE_MODIFICATION:
            case MODIFICATION:
                writerCount--;
                if (state == ComponentState.READABLE_WRITABLE && isFull()) {
                    state = ComponentState.READABLE_UNWRITABLE;
                }
                break;
            case SEARCH:
                readerCount--;
                if (state == ComponentState.UNREADABLE_UNWRITABLE && readerCount == 0) {
                    reset();
                    resetCallback.reset();
                    state = ComponentState.READABLE_WRITABLE;
                } else if (state == ComponentState.READABLE_WRITABLE && isFull()) {
                    state = ComponentState.READABLE_UNWRITABLE;
                }
                break;
            case FLUSH:
                if (failedOperation) {
                    state = isFull() ? ComponentState.READABLE_UNWRITABLE : ComponentState.READABLE_WRITABLE;
                }
                readerCount--;
                if (readerCount == 0) {
                    reset();
                    resetCallback.reset();
                    state = ComponentState.READABLE_WRITABLE;
                } else if (state == ComponentState.READABLE_UNWRITABLE_FLUSHING) {
                    state = ComponentState.UNREADABLE_UNWRITABLE;
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operation " + opType);
        }
        notifyAll();
    }

    public void registerOnResetCallback(IMutableResetCallback resetCallback) {
        this.resetCallback = resetCallback;
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
