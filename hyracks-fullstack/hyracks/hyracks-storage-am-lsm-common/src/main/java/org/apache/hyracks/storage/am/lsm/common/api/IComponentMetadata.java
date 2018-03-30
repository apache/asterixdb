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
package org.apache.hyracks.storage.am.lsm.common.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public interface IComponentMetadata {

    /**
     * Put the key value pair in this metadata, overwrite if it exists
     *
     * @param key
     * @param value
     * @throws HyracksDataException
     *             if the component is immutable
     */
    void put(IValueReference key, IValueReference value) throws HyracksDataException;

    /**
     * Get the value of the key from the metadata, 0 length value if not exists
     *
     * @param key
     * @param value
     * @throws HyracksDataException
     */
    void get(IValueReference key, ArrayBackedValueStorage value) throws HyracksDataException;
}
