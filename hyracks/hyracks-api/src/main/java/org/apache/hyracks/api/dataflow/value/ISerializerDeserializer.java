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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Serializable;

import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface ISerializerDeserializer<T> extends Serializable {
    /**
     * Deserialization method.
     *
     * @param in
     *            - Stream to read instance from.
     * @return A new instance of T with data.
     */
    public T deserialize(DataInput in) throws HyracksDataException;

    /**
     * Serialization method.
     *
     * @param instance
     *            - the instance to serialize.
     * @param out
     *            - Stream to write data to.
     */
    public void serialize(T instance, DataOutput out) throws HyracksDataException;

    /*
     * TODO: Add a new method:
     * T deserialize(DataInput in, T mutable)
     * to provide deserialization without creating objects
     */
}
