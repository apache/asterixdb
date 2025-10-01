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
package org.apache.hyracks.storage.common.file;

import java.util.Date;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.util.annotations.NotThreadSafe;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

@NotThreadSafe
public class FileMapManager implements IFileMapManager {
    private static final long serialVersionUID = 2L;
    private static final int NOT_FOUND = -1;

    private final Int2ObjectMap<FileReference> id2nameMap;
    private final Object2IntMap<FileReference> name2IdMap;
    private int idCounter = 0;

    public FileMapManager() {
        id2nameMap = new Int2ObjectOpenHashMap<>();
        name2IdMap = new Object2IntOpenHashMap<>();
        name2IdMap.defaultReturnValue(NOT_FOUND);
    }

    @Override
    public FileReference lookupFileName(int fileId) throws HyracksDataException {
        FileReference fRef = id2nameMap.get(fileId);
        if (fRef == null) {
            throw HyracksDataException.create(ErrorCode.NO_MAPPING_FOR_FILE_ID, fileId);
        }
        return fRef;
    }

    @Override
    public int lookupFileId(FileReference fileRef) throws HyracksDataException {
        int fileId = name2IdMap.getInt(fileRef);
        if (fileId == NOT_FOUND) {
            throw HyracksDataException.create(ErrorCode.NO_MAPPING_FOR_FILENAME, fileRef);
        }
        return fileId;
    }

    @Override
    public boolean isMapped(FileReference fileRef) {
        return name2IdMap.containsKey(fileRef);
    }

    @Override
    public boolean isMapped(int fileId) {
        return id2nameMap.containsKey(fileId);
    }

    @Override
    public FileReference unregisterFile(int fileId) throws HyracksDataException {
        FileReference fileRef = id2nameMap.remove(fileId);
        if (fileRef == null) {
            throw HyracksDataException.create(ErrorCode.NO_MAPPING_FOR_FILE_ID, fileId);
        }
        name2IdMap.removeInt(fileRef);
        fileRef.unregister();
        return fileRef;
    }

    @Override
    public int unregisterFile(FileReference fileRef) throws HyracksDataException {
        int fileId = name2IdMap.removeInt(fileRef);
        if (fileId == NOT_FOUND) {
            throw HyracksDataException.create(ErrorCode.NO_MAPPING_FOR_FILENAME, fileRef);
        }
        id2nameMap.remove(fileId);
        fileRef.unregister();
        return fileId;
    }

    @Override
    public int registerFile(FileReference fileRef) throws HyracksDataException {
        int existingKey = name2IdMap.getInt(fileRef);
        if (existingKey != NOT_FOUND) {
            FileReference prevFile = id2nameMap.get(existingKey);
            throw HyracksDataException.create(ErrorCode.FILE_ALREADY_MAPPED, fileRef, prevFile,
                    new Date(prevFile.registrationTime()).toString());
        }
        return registerFileSafe(fileRef);
    }

    private int registerFileSafe(FileReference fileRef) {
        int fileId = idCounter++;
        if (idCounter == NOT_FOUND) {
            idCounter++;
        }
        fileRef.register();
        id2nameMap.put(fileId, fileRef);
        name2IdMap.put(fileRef, fileId);
        return fileId;
    }

    @Override
    public int registerFileIfAbsent(FileReference fileRef) {
        int fileId = name2IdMap.getInt(fileRef);
        return fileId != NOT_FOUND ? fileId : registerFileSafe(fileRef);
    }

}
