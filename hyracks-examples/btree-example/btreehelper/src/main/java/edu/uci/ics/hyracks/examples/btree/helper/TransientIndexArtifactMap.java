/*
 * Copyright 2009-2012 by The Regents of the University of California
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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.storage.common.file.IIndexArtifactMap;

public class TransientIndexArtifactMap implements IIndexArtifactMap {
    private long counter = 0;
    private Map<String, Long> name2IdMap = new HashMap<String, Long>();

    @Override
    public long create(String baseDir, List<IODeviceHandle> IODeviceHandles) throws IOException {
        long resourceId = counter++;
        String fullDir;
        synchronized (name2IdMap) {
            for (IODeviceHandle dev : IODeviceHandles) {
                fullDir = dev.getPath().toString();
                if (!fullDir.endsWith(System.getProperty("file.separator"))) {
                    fullDir += System.getProperty("file.separator");
                }
                fullDir += baseDir;
                if (name2IdMap.containsKey(fullDir)) {
                    throw new IOException();
                }
                name2IdMap.put(fullDir, resourceId);
            }
        }
        return resourceId;
    }

    /**
     * Search and return the resourceId indicated by fullDir from in-memory hashMap, name2IdMap.
     * When there is no corresponding id in name2IdMap, return -1;
     */
    @Override
    public long get(String fullDir) {
        Long resourceId = -1L;

        synchronized (name2IdMap) {
            resourceId = name2IdMap.get(fullDir);
        }

        if (resourceId == null) {
            return -1;
        } else {
            return resourceId;
        }
    }

    @Override
    public void delete(String baseDir, List<IODeviceHandle> IODeviceHandles) {
        String fullDir;
        synchronized (name2IdMap) {
            for (IODeviceHandle dev : IODeviceHandles) {
                fullDir = dev.getPath().toString();
                if (!fullDir.endsWith(System.getProperty("file.separator"))) {
                    fullDir += System.getProperty("file.separator");
                }
                fullDir += baseDir;
                name2IdMap.remove(fullDir);
            }
        }
    }
}
