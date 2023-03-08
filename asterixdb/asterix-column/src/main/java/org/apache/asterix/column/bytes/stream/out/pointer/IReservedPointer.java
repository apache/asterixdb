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
package org.apache.asterix.column.bytes.stream.out.pointer;

import org.apache.asterix.column.bytes.stream.out.AbstractBytesOutputStream;

/**
 * Pointer that reference a position in {@link AbstractBytesOutputStream}
 */
public interface IReservedPointer {
    /**
     * Set a byte value at the pointer's position
     *
     * @param value byte value to be set
     */
    void setByte(byte value);

    /**
     * Set an integer value at the pointer's position
     *
     * @param value integer value to be set
     */
    void setInteger(int value);

    /**
     * Reset the pointer
     */
    void reset();

    /**
     * @return whether the pointer is set or not
     */
    boolean isSet();
}
