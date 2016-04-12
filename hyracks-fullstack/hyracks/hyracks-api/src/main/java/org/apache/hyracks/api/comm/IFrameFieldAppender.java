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

package org.apache.hyracks.api.comm;

import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * The IFrameFieldAppender is used to append the data into frame field by field.
 */
public interface IFrameFieldAppender extends IFrameAppender {

    /**
     * Append the field stored in {@code bytes} into the current frame.
     *
     * @param bytes  the byte array that stores the field data
     * @param offset the offset of the field data
     * @param length the length of the field data
     * @return true if the current frame has enough space to hold the field data, otherwise return false.
     * @throws HyracksDataException
     */
    boolean appendField(byte[] bytes, int offset, int length) throws HyracksDataException;

    /**
     * Append the field of {@code fid} from the tuple {@code tid} whose information is stored in the {@code accessor}
     * into the current frame.
     *
     * @param accessor tupleAccessor
     * @param tid      tuple id in tupleAccessor
     * @param fid      field id of the tuple {@code tid}
     * @return true if the current frame has enough space to hold the field data, otherwise return false.
     * @throws HyracksDataException
     */
    boolean appendField(IFrameTupleAccessor accessor, int tid, int fid) throws HyracksDataException;
}
