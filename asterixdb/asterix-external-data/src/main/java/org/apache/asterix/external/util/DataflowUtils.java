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
package org.apache.asterix.external.util;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class DataflowUtils {

    private DataflowUtils() {
    }

    public static void addTupleToFrame(FrameTupleAppender appender, ArrayTupleBuilder tb, IFrameWriter writer)
            throws HyracksDataException {
        if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
            appender.write(writer, true);
            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                throw new RuntimeDataException(ErrorCode.UTIL_DATAFLOW_UTILS_TUPLE_TOO_LARGE);
            }
        }
    }

    public static void addTupleToFrame(FrameTupleAppender appender, ITupleReference tuple, IFrameWriter writer)
            throws HyracksDataException {
        if (!appender.append(tuple)) {
            appender.write(writer, true);
            if (!appender.append(tuple)) {
                throw new RuntimeDataException(ErrorCode.UTIL_DATAFLOW_UTILS_TUPLE_TOO_LARGE);
            }
        }
    }
}
