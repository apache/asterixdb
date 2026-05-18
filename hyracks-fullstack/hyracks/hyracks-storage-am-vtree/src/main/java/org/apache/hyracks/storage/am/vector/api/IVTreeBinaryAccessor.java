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
package org.apache.hyracks.storage.am.vector.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Schema-agnostic accessor for a serialized vector field. Lets storage-layer code read
 * vectors without depending on AsterixDB type-system details.
 * <p>
 * Stateful (holds a reference to the last {@code reset} buffer) and not thread-safe; confine one
 * instance to a single operation context.
 */
public interface IVTreeBinaryAccessor {

    /**
     * Reset the accessor to point at a serialized vector field.
     *
     * @param data byte array containing the serialized vector
     * @param start start offset of the vector field
     * @param length length of the vector field in bytes
     * @throws HyracksDataException if the data format is invalid
     */
    void reset(byte[] data, int start, int length) throws HyracksDataException;

    /**
     * Decodes and returns the vector at the current {@code reset} position. Returns a freshly
     * allocated {@code double[]} on each call (not aliased to the backing buffer), so callers may
     * retain or mutate it freely.
     */
    double[] getVector() throws HyracksDataException;

    int getDimension() throws HyracksDataException;
}
