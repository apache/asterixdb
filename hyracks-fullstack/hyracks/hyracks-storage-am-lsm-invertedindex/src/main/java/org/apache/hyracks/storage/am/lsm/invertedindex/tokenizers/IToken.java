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

package org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers;

import java.io.IOException;

import org.apache.hyracks.data.std.util.GrowableArray;

public interface IToken {
    public byte[] getData();

    public int getEndOffset();

    public int getStartOffset();

    public int getTokenLength();

    /**
     * reset the storage byte array.
     *
     * @param data
     * @param startOffset
     * @param endOffset
     * @param tokenLength
     * @param tokenCount  the count of this token in a document , or a record, or something else.
     */
    public void reset(byte[] data, int startOffset, int endOffset, int tokenLength, int tokenCount);

    public void serializeToken(GrowableArray out) throws IOException;

    public void serializeTokenCount(GrowableArray out) throws IOException;
}
