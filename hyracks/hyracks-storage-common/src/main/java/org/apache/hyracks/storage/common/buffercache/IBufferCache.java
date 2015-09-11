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
package org.apache.hyracks.storage.common.buffercache;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.replication.IIOReplicationManager;

public interface IBufferCache {
    public void createFile(FileReference fileRef) throws HyracksDataException;

    public int createMemFile() throws HyracksDataException;

    public void openFile(int fileId) throws HyracksDataException;

    public void closeFile(int fileId) throws HyracksDataException;

    public void deleteFile(int fileId, boolean flushDirtyPages) throws HyracksDataException;

    public void deleteMemFile(int fileId) throws HyracksDataException;

    public ICachedPage tryPin(long dpid) throws HyracksDataException;

    public ICachedPage pin(long dpid, boolean newPage) throws HyracksDataException;

    public ICachedPage pinVirtual(long vpid) throws HyracksDataException;

    public ICachedPage unpinVirtual(long vpid, long dpid) throws HyracksDataException;

    public void unpin(ICachedPage page) throws HyracksDataException;

    public void flushDirtyPage(ICachedPage page) throws HyracksDataException;

    public void force(int fileId, boolean metadata) throws HyracksDataException;

    public int getPageSize();

    public int getNumPages();

    public int getFileReferenceCount(int fileId);

    public void close() throws HyracksDataException;
    
    public boolean isReplicationEnabled();

    public IIOReplicationManager getIIOReplicationManager();

}
