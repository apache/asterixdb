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
package org.apache.hyracks.data.std.util;

import org.apache.hyracks.data.std.api.IValueReference;

public class DataUtils {

    private DataUtils() {
    }

    /**
     * Copies the content of this pointable to the passed byte array.
     * the array is expected to be at least of length = length of this pointable
     *
     * @param value
     *            the value to be copied
     * @param copy
     *            the array to write into
     * @throws ArrayIndexOutOfBoundsException
     *             if the passed array size is smaller than length
     */
    public static void copyInto(IValueReference value, byte[] copy) {
        System.arraycopy(value.getByteArray(), value.getStartOffset(), copy, 0, value.getLength());
    }

    /**
     * Copies the content of this pointable to the passed byte array.
     * the array is expected to be at least of length = offset + length of this pointable
     *
     * @param value
     *            the value to be copied
     * @param copy
     *            the array to write into
     * @param offset
     *            the offset to start writing from
     * @throws ArrayIndexOutOfBoundsException
     *             if the passed array size - offset is smaller than length
     */
    public static void copyInto(IValueReference value, byte[] copy, int offset) {
        System.arraycopy(value.getByteArray(), value.getStartOffset(), copy, offset, value.getLength());
    }
}
