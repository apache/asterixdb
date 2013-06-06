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
package edu.uci.ics.hyracks.storage.common.buffercache;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;

public interface IBufferCache {
    public void createFile(FileReference fileRef) throws HyracksDataException;

    public void openFile(int fileId) throws HyracksDataException;

    public void closeFile(int fileId) throws HyracksDataException;

    public void deleteFile(int fileId, boolean flushDirtyPages) throws HyracksDataException;

    public ICachedPage tryPin(long dpid) throws HyracksDataException;

    public ICachedPage pin(long dpid, boolean newPage) throws HyracksDataException;

    public void unpin(ICachedPage page) throws HyracksDataException;

    public void flushDirtyPage(ICachedPage page) throws HyracksDataException;

    public void force(int fileId, boolean metadata) throws HyracksDataException;

    public int getPageSize();

    public int getNumPages();

    public void close() throws HyracksDataException;
}