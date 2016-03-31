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
package org.apache.hyracks.dataflow.common.comm.io;

import java.io.DataInputStream;

import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;

public class ResultFrameTupleAccessor extends FrameTupleAccessor {

    public ResultFrameTupleAccessor() {
        super(null);
    }

    @Override
    protected void prettyPrint(int tid, ByteBufferInputStream bbis, DataInputStream dis, StringBuilder sb) {
        sb.append(tid + ":(" + getTupleStartOffset(tid) + ", " + getTupleEndOffset(tid) + ")[");

        bbis.setByteBuffer(getBuffer(), getTupleStartOffset(tid));
        sb.append(dis);

        sb.append("]\n");
    }

    @Override
    public int getFieldCount() {
        return 1;
    }
}
