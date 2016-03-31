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
package org.apache.hyracks.data.std.collections.api;

/**
 * Represents an immutable vector of ValueReferences. Users of this interface make the assumption
 * that it provides random access to the items in it. In other words, each of the getXXX(index) calls
 * have O(1) complexity.
 *
 * @author vinayakb
 */
public interface IValueReferenceVector {
    /**
     * Number of items in this vector.
     *
     * @return - the number of items in this vector.
     */
    public int getSize();

    /**
     * Get the byte array that contains the item referenced by <code>index</code>
     *
     * @param index
     *            - Index of the item in the vector
     * @return Byte Array that contains the item.
     */
    public byte[] getBytes(int index);

    /**
     * Get the start offset of the item referenced by <code>index</code>
     *
     * @param index
     *            - Index of the item in the vector
     * @return Start Offset in the Byte Array of the item
     */
    public int getStart(int index);

    /**
     * Get the length of the item referenced by <code>index</code>
     *
     * @param index
     *            - Index of the item in the vector
     * @return Length in the Byte Array of the item
     */
    public int getLength(int index);
}
