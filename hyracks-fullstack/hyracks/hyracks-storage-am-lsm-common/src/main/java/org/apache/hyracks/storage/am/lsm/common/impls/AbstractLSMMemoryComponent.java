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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractLSMMemoryComponent extends AbstractLSMComponent implements ILSMMemoryComponent {

    private static final Logger LOGGER = LogManager.getLogger();
    protected final AtomicBoolean allocated;
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
        allocated = new AtomicBoolean();
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
        activate();
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

    private void activate() throws HyracksDataException {
        if (state == ComponentState.INACTIVE) {
            if (!allocated.get()) {
                doAllocate();
            }
            state = ComponentState.READABLE_WRITABLE;
            lsmIndex.getIOOperationCallback().recycled(this);
        }
    }

    @Override
    public boolean threadEnter(LSMOperationType opType, boolean isMutableComponent) throws HyracksDataException {
        activate();
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
                    if (state == ComponentState.READABLE_WRITABLE && !vbc.isFull(this) && !vbc.isFull()) {
                        // Even when the memory component has the writable state, vbc may be temporarily full
                        // or this memory component may be full.
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
    public boolean threadExit(LSMOperationType opType, boolean failedOperation, boolean isMutableComponent)
            throws HyracksDataException {
        boolean cleanup = false;
        switch (opType) {
            case FORCE_MODIFICATION:
            case MODIFICATION:
                if (isMutableComponent) {
                    writerCount--;
                    // A failed operation should not change the component state since it's better for
                    // the failed operation's effect to be no-op.
                    if (state == ComponentState.READABLE_WRITABLE && !failedOperation && vbc.isFull(this)) {
                        // only mark the component state as unwritable when this memory component
                        // is full
                        state = ComponentState.READABLE_UNWRITABLE;
                    }
                } else {
                    readerCount--;
                    if (state == ComponentState.UNREADABLE_UNWRITABLE && readerCount == 0) {
                        cleanup = true;
                    }
                }
                break;
            case SEARCH:
                readerCount--;
                if (state == ComponentState.UNREADABLE_UNWRITABLE && readerCount == 0) {
                    cleanup = true;
                }
                break;
            case FLUSH:
                if (state != ComponentState.READABLE_UNWRITABLE_FLUSHING) {
                    throw new IllegalStateException("Flush sees an illegal LSM memory compoenent state: " + state);
                }
                readerCount--;
                if (!failedOperation) {
                    // If flush failed, keep the component state to READABLE_UNWRITABLE_FLUSHING
                    // operation succeeded
                    state = ComponentState.UNREADABLE_UNWRITABLE;
                    if (readerCount == 0) {
                        cleanup = true;
                    }
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operation " + opType);
        }
        if (readerCount <= -1 || writerCount <= -1) {
            throw new IllegalStateException("Invalid reader or writer count " + readerCount + " - " + writerCount);
        }
        return cleanup;
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
    public final void reset() throws HyracksDataException {
        state = ComponentState.INACTIVE;
        isModified.set(false);
        metadata.reset();
        if (filter != null) {
            filter.reset();
        }
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

    @Override
    public void cleanup() throws HyracksDataException {
        if (allocated.get()) {
            getIndex().deactivate();
            getIndex().destroy();
            allocated.set(false);
        }
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
        vbc.open();
        vbc.register(this);
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
            allocated.set(true);
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
            vbc.unregister(this);
        } finally {
            getIndex().getBufferCache().close();
        }
    }

    protected void doDeallocate() throws HyracksDataException {
        if (allocated.get()) {
            getIndex().deactivate();
            getIndex().destroy();
            allocated.set(false);
        }
        componentId = null;
    }

    @Override
    public void validate() throws HyracksDataException {
        getIndex().validate();
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
    public void flushed() throws HyracksDataException {
        vbc.flushed(this);
    }

    @Override
    public String toString() {
        return "{\"class\":\"" + getClass().getSimpleName() + "\", \"state\":\"" + state + "\", \"writers\":"
                + writerCount + ", \"readers\":" + readerCount + ", \"pendingFlushes\":" + pendingFlushes
                + ", \"id\":\"" + componentId + "\"}";
    }
}
