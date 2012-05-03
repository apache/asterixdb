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
package edu.uci.ics.asterix.runtime.transaction;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.resource.TransactionalResourceRepository;
import edu.uci.ics.asterix.transaction.management.service.logging.DataUtil;
import edu.uci.ics.asterix.transaction.management.service.logging.ILogRecordHelper;
import edu.uci.ics.asterix.transaction.management.service.logging.LogicalLogLocator;
import edu.uci.ics.asterix.transaction.management.service.transaction.IResourceManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;

public class TreeResourceManager implements IResourceManager {

    public static final byte ID = (byte) 1;

    private static final TreeResourceManager treeResourceMgr = new TreeResourceManager();

    private TreeResourceManager() {
    }

    public static TreeResourceManager getInstance() {
        return treeResourceMgr;
    }

    public byte getResourceManagerId() {
        return ID;
    }

    public void undo(ILogRecordHelper logRecordHelper, LogicalLogLocator logLocator) throws ACIDException {

        int logContentBeginPos = logRecordHelper.getLogContentBeginPos(logLocator);
        byte[] logBufferContent = logLocator.getBuffer().getArray();
        // read the length of resource id byte array
        int resourceIdLength = DataUtil.byteArrayToInt(logBufferContent, logContentBeginPos);
        byte[] resourceIdBytes = new byte[resourceIdLength];

        // copy the resource if bytes
        System.arraycopy(logBufferContent, logContentBeginPos + 4, resourceIdBytes, 0, resourceIdLength);

        // look up the repository to obtain the resource object
        ITreeIndex treeIndex = (ITreeIndex) TransactionalResourceRepository.getTransactionalResource(resourceIdBytes);
        int operationOffset = logContentBeginPos + 4 + resourceIdLength;
        int tupleBeginPos = operationOffset + 1;

        ITreeIndexTupleReference tupleReference = treeIndex.getLeafFrameFactory().getTupleWriterFactory()
                .createTupleWriter().createTupleReference();
        // TODO: remove this call.
        tupleReference.setFieldCount(tupleReference.getFieldCount());
        tupleReference.resetByTupleOffset(logLocator.getBuffer().getByteBuffer(), tupleBeginPos);
        byte operation = logBufferContent[operationOffset];
        IIndexAccessor treeIndexAccessor = treeIndex.createAccessor();
        try {
            switch (operation) {
                case TreeLogger.BTreeOperationCodes.INSERT:
                    treeIndexAccessor.delete(tupleReference);
                    break;
                case TreeLogger.BTreeOperationCodes.DELETE:
                    treeIndexAccessor.insert(tupleReference);
                    break;
            }
        } catch (Exception e) {
            throw new ACIDException(" could not rollback ", e);
        }
    }

    public void redo(ILogRecordHelper logRecordHelper, LogicalLogLocator logicalLogLocator) throws ACIDException {
        throw new UnsupportedOperationException(" Redo logic will be implemented as part of crash recovery feature");
    }

}
