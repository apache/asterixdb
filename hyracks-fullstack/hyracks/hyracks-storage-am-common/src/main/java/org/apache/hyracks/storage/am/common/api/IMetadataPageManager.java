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
package org.apache.hyracks.storage.am.common.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;

/**
 * Used to read from and write to index metadata.
 * The index metadata contains key-value pairs
 */
public interface IMetadataPageManager extends IPageManager {
    /**
     * put the key value pair in the metadata page using the passed frame
     * @param frame
     * @param key
     * @param value
     * @throws HyracksDataException
     */
    void put(ITreeIndexMetadataFrame frame, IValueReference key, IValueReference value) throws HyracksDataException;

    /**
     * get the value of the key from the metadata page using the passed frame
     * @param frame
     * @param key
     * @param value
     * @throws HyracksDataException
     */
    void get(ITreeIndexMetadataFrame frame, IValueReference key, IPointable value) throws HyracksDataException;

    /**
     * @param frame
     * @param key
     * @return The byte offset in the index file for the entry with the passed key if the index is valid and the key
     *         exists, returns -1 otherwise. use the passed frame to read the metadata page
     * @throws HyracksDataException
     */
    long getFileOffset(ITreeIndexMetadataFrame frame, IValueReference key) throws HyracksDataException;
}
