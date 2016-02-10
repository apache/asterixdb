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
package org.apache.hyracks.api.dataflow.value;

import java.io.Serializable;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IResultSerializer extends Serializable {
    /**
     * Initializes the serializer.
     */
    public void init() throws HyracksDataException;

    /**
     * Method to serialize the result and append it to the provided output stream
     *
     * @param tAccess
     *            - A frame tuple accessor object that contains the original data to be serialized
     * @param tIdx
     *            - Index of the tuple that should be serialized.
     * @return true if the tuple was appended successfully, else false.
     */
    public boolean appendTuple(IFrameTupleAccessor tAccess, int tIdx) throws HyracksDataException;
}
