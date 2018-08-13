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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId.IdCompareResult;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.am.lsm.common.util.LSMComponentIdUtils;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractLSMMemoryComponent extends AbstractLSMComponent implements ILSMMemoryComponent {

    private static final Logger LOGGER = LogManager.getLogger();
    private final IVirtualBufferCache vbc;
    private final AtomicBoolean isModified;
    private int writerCount;
    private int pendingFlushes = 0;
    private final MemoryComponentMetadata metadata;
    private ILSMComponentId componentId;

    public AbstractLSMMemoryComponent(AbstractLSMIndex lsmIndex, IVirtualBufferCache vbc, ILSMComponentFilter filter) {
        super(lsmIndex, filter);
        this.vbc = vbc;
        writerCount = 0;
        state = ComponentState.INACTIVE;
        isModified = new AtomicBoolean();
        metadata = new MemoryComponentMetadata();
    }

    /**
     * Prepare the component to be scheduled for an IO operation
     *
     * @param ioOperationType
     * @throws HyracksDataException
     */
    @Override
    public void schedule(LSMIOOperationType ioOperationType) throws HyracksDataException {
        activeate();
        if (ioOperationType == LSMIOOperationType.FLUSH) {
            if (state == ComponentState.READABLE_WRITABLE || state == ComponentState.READABLE_UNWRITABLE) {
                if (writerCount != 0) {
                    throw new IllegalStateException("Trying to schedule a flush when writerCount != 0");
                }
                state = ComponentState.READABLE_UNWRITABLE_FLUSHING;
            } else if (state == ComponentState.READABLE_UNWRITABLE_FLUSHING
                    || state == ComponentState.UNREADABLE_UNWRITABLE) {
                // There is an ongoing flush. Increase pending flush count
                pendingFlushes++;
            } else {
                throw new IllegalStateException("Trying to schedule a flush when the component state = " + state);
            }
        } else {
            throw new UnsupportedOperationException("Unsupported operation " + ioOperationType);
        }
    }

    private void activeate() throws HyracksDataException {
        if (state == ComponentState.INACTIVE) {
            state = ComponentState.READABLE_WRITABLE;
            lsmIndex.getIOOperationCallback().recycled(this);
        }
    }

    @Override
    public boolean threadEnter(LSMOperationType opType, boolean isMutableComponent) throws HyracksDataException {
        activeate();
        switch (opType) {
            case FORCE_MODIFICATION:
                if (isMutableComponent) {
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
                if (isMutableComponent) {
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
                if (state == ComponentState.UNREADABLE_UNWRITABLE) {
                    return false;
                }
                if (state != ComponentState.READABLE_UNWRITABLE_FLUSHING) {
                    throw new IllegalStateException("Trying to flush when component state = " + state);
                }
                if (writerCount != 0) {
                    throw new IllegalStateException("Trying to flush when writerCount = " + writerCount);
                }
                readerCount++;
                return true;

            default:
                throw new UnsupportedOperationException("Unsupported operation " + opType);
        }
        return true;
    }

    @Override
    public void threadExit(LSMOperationType opType, boolean failedOperation, boolean isMutableComponent)
            throws HyracksDataException {
        switch (opType) {
            case FORCE_MODIFICATION:
            case MODIFICATION:
                if (isMutableComponent) {
                    writerCount--;
                    // A failed operation should not change the component state since it's better for
                    // the failed operation's effect to be no-op.
                    if (state == ComponentState.READABLE_WRITABLE && !failedOperation && isFull()) {
                        state = ComponentState.READABLE_UNWRITABLE;
                    }
                } else {
                    readerCount--;
                    if (state == ComponentState.UNREADABLE_UNWRITABLE && readerCount == 0) {
                        reset();
                    }
                }
                break;
            case SEARCH:
                readerCount--;
                if (state == ComponentState.UNREADABLE_UNWRITABLE && readerCount == 0) {
                    reset();
                }
                break;
            case FLUSH:
                if (state != ComponentState.READABLE_UNWRITABLE_FLUSHING) {
                    throw new IllegalStateException("Flush sees an illegal LSM memory compoenent state: " + state);
                }
                readerCount--;
                if (failedOperation) {
                    // If flush failed, keep the component state to READABLE_UNWRITABLE_FLUSHING
                    return;
                }
                // operation succeeded
                if (readerCount == 0) {
                    reset();
                } else {
                    state = ComponentState.UNREADABLE_UNWRITABLE;
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operation " + opType);
        }

        if (readerCount <= -1 || writerCount <= -1) {
            throw new IllegalStateException("Invalid reader or writer count " + readerCount + " - " + writerCount);
        }
    }

    @Override
    public boolean isReadable() {
        return state != ComponentState.INACTIVE && state != ComponentState.UNREADABLE_UNWRITABLE;
    }

    @Override
    public void setUnwritable() {
        if (state != ComponentState.READABLE_WRITABLE) {
            throw new IllegalStateException("Attempt to set unwritable a component that is " + state);
        }
        this.state = ComponentState.READABLE_UNWRITABLE;
    }

    @Override
    public void setModified() {
        isModified.set(true);
    }

    @Override
    public boolean isModified() {
        return isModified.get();
    }

    @Override
    public boolean isFull() {
        return vbc.isFull();
    }

    @Override
    public final void reset() throws HyracksDataException {
        state = ComponentState.INACTIVE;
        isModified.set(false);
        metadata.reset();
        if (filter != null) {
            filter.reset();
        }
        doReset();
        lsmIndex.memoryComponentsReset();
        // a flush can be pending on a component that just completed its flush... here is when this can happen:
        // primary index has 2 components, secondary index has 2 components.
        // 2 flushes are scheduled on each p1, p2, s1, and s2.
        // p1 and p2 both finish. primary component 1 gets full and secondary doesn't have any entries (optional field).
        // then flush is scheduled on p1, s1 will have a pending flush in that case.
        if (pendingFlushes > 0) {
            schedule(LSMIOOperationType.FLUSH);
            pendingFlushes--;
        }
    }

    protected void doReset() throws HyracksDataException {
        getIndex().deactivate();
        getIndex().destroy();
        getIndex().create();
        getIndex().activate();
    }

    @Override
    public int getWriterCount() {
        return writerCount;
    }

    @Override
    public MemoryComponentMetadata getMetadata() {
        return metadata;
    }

    @Override
    public final void allocate() throws HyracksDataException {
        boolean allocated = false;
        ((IVirtualBufferCache) getIndex().getBufferCache()).open();
        try {
            doAllocate();
            allocated = true;
        } finally {
            if (!allocated) {
                getIndex().getBufferCache().close();
            }
        }
    }

    protected void doAllocate() throws HyracksDataException {
        boolean created = false;
        boolean activated = false;
        try {
            getIndex().create();
            created = true;
            getIndex().activate();
            activated = true;
        } finally {
            if (created && !activated) {
                getIndex().destroy();
            }
        }
    }

    @Override
    public final void deallocate() throws HyracksDataException {
        try {
            state = ComponentState.INACTIVE;
            doDeallocate();
        } finally {
            getIndex().getBufferCache().close();
        }
    }

    protected void doDeallocate() throws HyracksDataException {
        getIndex().deactivate();
        getIndex().destroy();
        componentId = null;
    }

    @Override
    public void validate() throws HyracksDataException {
        getIndex().validate();
    }

    @Override
    public long getSize() {
        IBufferCache virtualBufferCache = getIndex().getBufferCache();
        return virtualBufferCache.getPageBudget() * (long) virtualBufferCache.getPageSize();
    }

    @Override
    public ILSMComponentId getId() {
        return componentId;
    }

    @Override
    public void resetId(ILSMComponentId componentId, boolean force) throws HyracksDataException {
        if (!force && this.componentId != null
                && this.componentId.compareTo(componentId) != IdCompareResult.LESS_THAN) {
            throw new IllegalStateException(
                    this + " receives illegal id. Old id " + this.componentId + ", new id " + componentId);
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Component Id was reset from " + this.componentId + " to " + componentId);
        }
        this.componentId = componentId;
        if (componentId != null) {
            LSMComponentIdUtils.persist(this.componentId, metadata);
        }
    }

    @Override
    public String toString() {
        return "{\"class\":\"" + getClass().getSimpleName() + "\", \"state\":\"" + state + "\", \"writers\":"
                + writerCount + ", \"readers\":" + readerCount + ", \"pendingFlushes\":" + pendingFlushes
                + ", \"id\":\"" + componentId + "\"}";
    }
}
