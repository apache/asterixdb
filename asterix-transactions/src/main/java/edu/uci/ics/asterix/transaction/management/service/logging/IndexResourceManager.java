/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.transaction.management.service.logging;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.transaction.IResourceManager;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionSubsystem;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.common.tuples.SimpleTupleReference;
import edu.uci.ics.hyracks.storage.am.common.tuples.SimpleTupleWriter;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;

public class IndexResourceManager implements IResourceManager {

    public final byte resourceType;

    private final TransactionSubsystem provider;

    public IndexResourceManager(byte resourceType, TransactionSubsystem provider) {
        this.resourceType = resourceType;
        this.provider = provider;
    }

    public byte getResourceManagerId() {
        return resourceType;
    }

    public void undo(ILogRecordHelper logRecordHelper, LogicalLogLocator logLocator) throws ACIDException {
        long resourceId = logRecordHelper.getResourceId(logLocator);
        int offset = logRecordHelper.getLogContentBeginPos(logLocator);

        //TODO
        //replace TransactionResourceRepository with IndexLifeCycleManager
        // look up the repository to obtain the resource object
        IIndex index = (IIndex) provider.getTransactionalResourceRepository().getTransactionalResource(resourceId);

        /* field count */
        int fieldCount = logLocator.getBuffer().readInt(logLocator.getMemoryOffset() + offset);
        offset += 4;

        /* new operation */
        byte newOperation = logLocator.getBuffer().getByte(logLocator.getMemoryOffset() + offset);
        offset += 1;

        /* new value size */
        int newValueSize = logLocator.getBuffer().readInt(logLocator.getMemoryOffset() + offset);
        offset += 4;

        /* new value */
        SimpleTupleWriter tupleWriter = new SimpleTupleWriter();
        SimpleTupleReference newTuple = (SimpleTupleReference) tupleWriter.createTupleReference();
        newTuple.setFieldCount(fieldCount);
        newTuple.resetByTupleOffset(logLocator.getBuffer().getByteBuffer(), offset);
        offset += newValueSize;

        ILSMIndexAccessor indexAccessor = (ILSMIndexAccessor) index.createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);

        try {
            if (resourceType == ResourceType.LSM_BTREE) {

                /* old operation */
                byte oldOperation = logLocator.getBuffer().getByte(logLocator.getMemoryOffset() + offset);
                offset += 1;

                if (oldOperation != (byte) IndexOperation.NOOP.ordinal()) {
                    /* old value size */
                    int oldValueSize = logLocator.getBuffer().readInt(logLocator.getMemoryOffset() + offset);
                    offset += 4;

                    /* old value */
                    SimpleTupleReference oldTuple = (SimpleTupleReference) tupleWriter.createTupleReference();
                    oldTuple.setFieldCount(fieldCount);
                    oldTuple.resetByTupleOffset(logLocator.getBuffer().getByteBuffer(), offset);
                    offset += oldValueSize;

                    if (oldOperation == (byte) IndexOperation.DELETE.ordinal()) {
                        indexAccessor.forceDelete(oldTuple);
                    } else {
                        indexAccessor.forceInsert(oldTuple);
                    }
                } else {
                    indexAccessor.forcePhysicalDelete(newTuple);
                }
            } else {
                if (newOperation == (byte) IndexOperation.DELETE.ordinal()) {
                    indexAccessor.forceInsert(newTuple);
                } else {
                    indexAccessor.forceDelete(newTuple);
                }
            }
        } catch (Exception e) {
            throw new ACIDException("Undo failed", e);
        }
    }

    public void redo(ILogRecordHelper logRecordHelper, LogicalLogLocator logLocator) throws ACIDException {
        long resourceId = logRecordHelper.getResourceId(logLocator);
        int offset = logRecordHelper.getLogContentBeginPos(logLocator);

        IIndex index = (IIndex) provider.getAsterixAppRuntimeContextProvider().getIndexLifecycleManager()
                .getIndex(resourceId);

        /* field count */
        int fieldCount = logLocator.getBuffer().readInt(logLocator.getMemoryOffset() + offset);
        offset += 4;

        /* new operation */
        byte newOperation = logLocator.getBuffer().getByte(logLocator.getMemoryOffset() + offset);
        offset += 1;

        /* new value size */
        int newValueSize = logLocator.getBuffer().readInt(logLocator.getMemoryOffset() + offset);
        offset += 4;

        /* new value */
        SimpleTupleWriter tupleWriter = new SimpleTupleWriter();
        SimpleTupleReference newTuple = (SimpleTupleReference) tupleWriter.createTupleReference();
        newTuple.setFieldCount(fieldCount);
        newTuple.resetByTupleOffset(logLocator.getBuffer().getByteBuffer(), offset);
        offset += newValueSize;

        ILSMIndexAccessor indexAccessor = (ILSMIndexAccessor) index.createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);

        try {
            if (newOperation == IndexOperation.INSERT.ordinal()) {
                indexAccessor.insert(newTuple);
            } else if (newOperation == IndexOperation.DELETE.ordinal()) {
                indexAccessor.delete(newTuple);
            } else {
                new ACIDException("Unsupported operation type for undo operation : " + newOperation);
            }
        } catch (Exception e) {
            throw new ACIDException("Redo failed", e);
        }
    }

}
