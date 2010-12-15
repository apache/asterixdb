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

import java.io.Serializable;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IFileMapProvider extends Serializable {
    /**
     * Indicates if a given fileId is mapped
     * 
     * @param fileId
     * @return <code>true</code> if the given fileId is mapped, <code>false</code> otherwise.
     */
    public boolean isMapped(int fileId);

    /**
     * Indicates if a given file name is mapped.
     * 
     * @param fileName
     * @return <code>true</code> if the given file name is mapped, <code>false</code> otherwise.
     */
    public boolean isMapped(String fileName);

    /**
     * Lookup the file id for a file name
     * 
     * @param fileName
     *            - The file name whose id should be looked up.
     * @return The file id
     * @throws HyracksDataException
     *             - If the file name is not currently mapped in this manager.
     */
    public int lookupFileId(String fileName) throws HyracksDataException;

    /**
     * Lookup the file name for a file id
     * 
     * @param fileId
     *            - The file id whose name should be looked up.
     * @return The file name
     * @throws HyracksDataException
     *             - If the file id is not mapped currently in this manager.
     */
    public String lookupFileName(int fileId) throws HyracksDataException;
}