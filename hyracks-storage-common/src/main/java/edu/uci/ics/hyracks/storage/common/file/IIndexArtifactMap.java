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

package edu.uci.ics.hyracks.storage.common.file;

import java.io.IOException;
import java.util.List;

import edu.uci.ics.hyracks.api.io.IODeviceHandle;

/**
 * IIndexArtifactMap provides interface to create an index artifact(that is a resourceId, but can be generalized to accommodate any artifact later) and retrieve it.
 * 
 * @author kisskys
 */
public interface IIndexArtifactMap {
    /**
     * Creates an artifact(resourceId) indicated by @param baseDir and @param ioDeviceHandles and return it.
     * 
     * @param baseDir
     * @param ioDeviceHandles
     * @return a created artifact if it is created successfully.
     * @throws IOException
     *             if the corresponding artifact already exists
     */
    public long create(String baseDir, List<IODeviceHandle> ioDeviceHandles) throws IOException;

    /**
     * Retrieves the artifact(resourceId) indicated by @param fullDir
     * 
     * @param fullDir
     * @return the retrieved artifact if it exists, -1 otherwise.
     */
    public long get(String fullDir);

    /**
     * Deletes an artifact(resourceId) indicated by @param baseDir and @param ioDeviceHandles.
     * 
     * @param fullDir
     */
    public void delete(String baseDir, List<IODeviceHandle> ioDeviceHandles);
}
