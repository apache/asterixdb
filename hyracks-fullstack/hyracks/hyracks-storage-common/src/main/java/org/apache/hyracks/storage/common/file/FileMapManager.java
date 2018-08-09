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
import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.util.annotations.NotThreadSafe;

@NotThreadSafe
public class FileMapManager implements IFileMapManager {
    private static final long serialVersionUID = 1L;

    private Map<Integer, FileReference> id2nameMap = new HashMap<>();
    private Map<FileReference, Integer> name2IdMap = new HashMap<>();
    private int idCounter = 0;

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
        Integer fileId = name2IdMap.get(fileRef);
        if (fileId == null) {
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
        name2IdMap.remove(fileRef);
        fileRef.unregister();
        return fileRef;
    }

    @Override
    public int registerFile(FileReference fileRef) throws HyracksDataException {
        Integer existingKey = name2IdMap.get(fileRef);
        if (existingKey != null) {
            FileReference prevFile = id2nameMap.get(existingKey);
            throw HyracksDataException.create(ErrorCode.FILE_ALREADY_MAPPED, fileRef, prevFile,
                    new Date(prevFile.registrationTime()).toString());
        }
        int fileId = idCounter++;
        fileRef.register();
        id2nameMap.put(fileId, fileRef);
        name2IdMap.put(fileRef, fileId);
        return fileId;
    }

}
