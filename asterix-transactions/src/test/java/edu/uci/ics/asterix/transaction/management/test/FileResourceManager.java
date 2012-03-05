/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.asterix.transaction.management.test;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.logging.IResource;
import edu.uci.ics.asterix.transaction.management.service.logging.ILogRecordHelper;
import edu.uci.ics.asterix.transaction.management.service.logging.LogicalLogLocator;
import edu.uci.ics.asterix.transaction.management.service.transaction.IResourceManager;

class FileResourceManager implements IResourceManager {

    public static final byte id = 100;
    private Map<String, FileResource> transactionalResources = new HashMap<String, FileResource>();

    public void registerTransactionalResource(IResource resource) throws ACIDException {
        if (resource instanceof FileResource) {
            if (transactionalResources.get(new String(resource.getId())) == null) {
                transactionalResources.put(new String(resource.getId()), (FileResource) resource);
            }
        } else {
            throw new ACIDException(" invalid resource type :" + resource);
        }
    }

    @Override
    public byte getResourceManagerId() {
        return id;
    }

    @Override
    public void undo(ILogRecordHelper logRecordHelper, LogicalLogLocator memLSN) throws ACIDException {
        LogRecordInfo logRecordInfo = new LogRecordInfo(logRecordHelper, memLSN);
        FileResource fileManager = transactionalResources.get(logRecordInfo.getResourceId());
        if (fileManager == null) {
            throw new ACIDException(" Un-registered transactional resource :" + logRecordInfo.getResourceId());
        }
        fileManager.setValue(logRecordInfo.getBeforeValue());
    }

    @Override
    public void redo(ILogRecordHelper logRecordHelper, LogicalLogLocator memLSN) throws ACIDException {
        LogRecordInfo logRecordInfo = new LogRecordInfo(logRecordHelper, memLSN);
        FileResource fileManager = transactionalResources.get(new String(logRecordInfo.getResourceId()));
        if (fileManager == null) {
            throw new ACIDException(" Un-registered transactional resource :" + logRecordInfo.getResourceId());
        }
        fileManager.setValue(logRecordInfo.getAfterValue());
    }

}

class LogRecordInfo {

    byte[] resourceId;
    int beforeValue;
    int afterValue;

    public LogRecordInfo(ILogRecordHelper logParser, LogicalLogLocator memLSN) throws ACIDException {
        int logContentBeginPos = logParser.getLogContentBeginPos(memLSN);
        int logContentEndPos = logParser.getLogContentEndPos(memLSN);
        byte[] bufferContent = memLSN.getBuffer().getArray();
        resourceId = new byte[] { bufferContent[logContentBeginPos] };
        String content = new String(bufferContent, logContentBeginPos + resourceId.length, logContentEndPos
                - (logContentBeginPos + resourceId.length));
        beforeValue = Integer.parseInt(content.split(" ")[0]);
        afterValue = Integer.parseInt(content.split(" ")[1]);
    }

    public byte[] getResourceId() {
        return resourceId;
    }

    public void setResourceId(byte[] resourceId) {
        this.resourceId = resourceId;
    }

    public int getAfterValue() {
        return afterValue;
    }

    public void setFinalValue(int afterValue) {
        this.afterValue = afterValue;
    }

    public int getBeforeValue() {
        return beforeValue;
    }

    public void setBeforeValue(int beforeValue) {
        this.beforeValue = beforeValue;
    }

}