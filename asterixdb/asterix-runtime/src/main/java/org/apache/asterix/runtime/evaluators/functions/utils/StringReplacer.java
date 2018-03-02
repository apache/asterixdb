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

package org.apache.asterix.runtime.evaluators.functions.utils;

import java.io.IOException;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.data.std.util.UTF8StringBuilder;

/**
 * A wrapper for string replace methods.
 */
public final class StringReplacer {
    // For outputting the result.
    private final UTF8StringBuilder resultBuilder = new UTF8StringBuilder();
    private final GrowableArray resultArray = new GrowableArray();
    private final int resultArrayInitLength;

    public StringReplacer() throws HyracksDataException {
        try {
            resultArray.getDataOutput().writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
            resultArrayInitLength = resultArray.getLength();
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public boolean findAndReplace(UTF8StringPointable input, UTF8StringPointable search, UTF8StringPointable replace,
            int limit) throws HyracksDataException {
        try {
            resultArray.setSize(resultArrayInitLength);
            return input.findAndReplace(search, replace, limit, resultBuilder, resultArray);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public void assignResult(IPointable resultPointable) {
        resultPointable.set(resultArray.getByteArray(), 0, resultArray.getLength());
    }
}
