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
package org.apache.asterix.external.api;

/**
 * This interface represents a raw record that is not parsed yet.
 * @param <T>
 */
public interface IRawRecord<T> {
    /**
     * @return the bytes representing this record. This is intended to be used for passing raw records in frames and
     *         performing lazy deserialization on them. If the record can't be serialized, this method returns null.
     */
    public byte[] getBytes();

    /**
     * @return the java object of the record.
     */
    public T get();

    /**
     * Resets the object to prepare it for another write operation.
     */
    public void reset();

    /**
     * @return The size of the valid bytes of the object. If the object can't be serialized, this method returns -1
     */
    int size();

    /**
     * Sets the new value of the record
     * @param t
     */
    public void set(T t);
}
