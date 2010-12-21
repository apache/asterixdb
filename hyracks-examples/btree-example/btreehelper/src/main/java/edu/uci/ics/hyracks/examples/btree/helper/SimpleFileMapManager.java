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

package edu.uci.ics.hyracks.examples.btree.helper;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.common.file.IFileMapManager;

public class SimpleFileMapManager implements IFileMapManager {

    private static final long serialVersionUID = 1L;
    private Map<Integer, String> id2nameMap = new HashMap<Integer, String>();
    private Map<String, Integer> name2IdMap = new HashMap<String, Integer>();
    private int idCounter = 0;

    @Override
    public String lookupFileName(int fileId) throws HyracksDataException {
        String fName = id2nameMap.get(fileId);
        if (fName == null) {
            throw new HyracksDataException("No mapping found for id: " + fileId);
        }
        return fName;
    }

    @Override
    public int lookupFileId(String fileName) throws HyracksDataException {
        Integer fileId = name2IdMap.get(fileName);
        if (fileId == null) {
            throw new HyracksDataException("No mapping found for name: " + fileName);
        }
        return fileId;
    }

    @Override
    public boolean isMapped(String fileName) {
        return name2IdMap.containsKey(fileName);
    }

    @Override
    public boolean isMapped(int fileId) {
        return id2nameMap.containsKey(fileId);
    }

    @Override
    public void unregisterFile(int fileId) throws HyracksDataException {
        String fileName = id2nameMap.remove(fileId);
        name2IdMap.remove(fileName);
    }

    @Override
    public void registerFile(String fileName) throws HyracksDataException {
        Integer fileId = idCounter++;
        id2nameMap.put(fileId, fileName);
        name2IdMap.put(fileName, fileId);
    }
}
