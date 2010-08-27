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
package edu.uci.ics.hyracks.storage.common.file;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class FileManager {
    private final Map<Integer, FileInfo> fileRegistry;

    public FileManager() {
        fileRegistry = new HashMap<Integer, FileInfo>();
    }

    public void registerFile(FileInfo fInfo) throws HyracksDataException {
        if (fileRegistry.containsKey(fInfo.getFileId())) {
            throw new HyracksDataException("File with id " + fInfo.getFileId() + " is already registered");
        }
        fileRegistry.put(fInfo.getFileId(), fInfo);
    }

    public FileInfo unregisterFile(int fileId) throws HyracksDataException {
        if (!fileRegistry.containsKey(fileId)) {
            throw new HyracksDataException("File with id " + fileId + " not in registry");
        }
        return fileRegistry.remove(fileId);
    }

    public FileInfo getFileInfo(int fileId) throws HyracksDataException {
        FileInfo fInfo = fileRegistry.get(fileId);
        if (fInfo == null) {
            throw new HyracksDataException("File with id " + fileId + " not in registry");
        }
        return fInfo;
    }

    public void close() {
        for (FileInfo fInfo : fileRegistry.values()) {
            try {
                fInfo.getFileChannel().close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}