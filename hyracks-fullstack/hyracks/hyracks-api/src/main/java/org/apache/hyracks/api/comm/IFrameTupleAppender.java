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

public interface IFrameTupleAppender extends IFrameAppender {

    boolean append(IFrameTupleAccessor tupleAccessor, int tIndex) throws HyracksDataException;

    boolean append(int[] fieldSlots, byte[] bytes, int offset, int length) throws HyracksDataException;

    boolean append(byte[] bytes, int offset, int length) throws HyracksDataException;

    boolean appendSkipEmptyField(int[] fieldSlots, byte[] bytes, int offset, int length) throws HyracksDataException;

    boolean append(IFrameTupleAccessor tupleAccessor, int tStartOffset, int tEndOffset) throws HyracksDataException;

    boolean appendConcat(IFrameTupleAccessor accessor0, int tIndex0, IFrameTupleAccessor accessor1, int tIndex1)
            throws HyracksDataException;

    boolean appendConcat(IFrameTupleAccessor accessor0, int tIndex0, int[] fieldSlots1, byte[] bytes1, int offset1,
            int dataLen1) throws HyracksDataException;

    boolean appendProjection(IFrameTupleAccessor accessor, int tIndex, int[] fields) throws HyracksDataException;
}
