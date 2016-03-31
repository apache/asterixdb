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

import java.io.Serializable;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;

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
     * @param fileRef
     * @return <code>true</code> if the given file name is mapped, <code>false</code> otherwise.
     */
    public boolean isMapped(FileReference fileRef);

    /**
     * Lookup the file id for a file name
     *
     * @param fileRef
     *            - The file name whose id should be looked up.
     * @return The file id
     * @throws HyracksDataException
     *             - If the file name is not currently mapped in this manager.
     */
    public int lookupFileId(FileReference fileRef) throws HyracksDataException;

    /**
     * Lookup the file name for a file id
     *
     * @param fileId
     *            - The file id whose name should be looked up.
     * @return The file reference
     * @throws HyracksDataException
     *             - If the file id is not mapped currently in this manager.
     */
    public FileReference lookupFileName(int fileId) throws HyracksDataException;

}
