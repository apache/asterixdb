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

package org.apache.asterix.runtime.evaluators.functions.binary;

import java.io.IOException;

import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder;

public abstract class AbstractSubBinaryEvaluator extends AbstractBinaryScalarEvaluator {

    private ByteArrayPointable byteArrayPointable = new ByteArrayPointable();
    private byte[] metaBuffer = new byte[5];
    protected final String functionName;

    private static final ATypeTag[] EXPECTED_INPUT_TAGS = { ATypeTag.BINARY, ATypeTag.INT32 };

    public AbstractSubBinaryEvaluator(IHyracksTaskContext context, IScalarEvaluatorFactory[] copyEvaluatorFactories,
            String functionName) throws HyracksDataException {
        super(context, copyEvaluatorFactories);
        this.functionName = functionName;
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        resultStorage.reset();
        for (int i = 0; i < pointables.length; ++i) {
            evaluators[i].evaluate(tuple, pointables[i]);
        }

        try {
            ATypeTag argTag0 = ATypeTag.VALUE_TYPE_MAPPING[pointables[0].getByteArray()[pointables[0]
                    .getStartOffset()]];
            ATypeTag argTag1 = ATypeTag.VALUE_TYPE_MAPPING[pointables[1].getByteArray()[pointables[1]
                    .getStartOffset()]];
            checkTypeMachingThrowsIfNot(functionName, EXPECTED_INPUT_TAGS, argTag0, argTag1);

            byteArrayPointable.set(pointables[0].getByteArray(), pointables[0].getStartOffset() + 1,
                    pointables[0].getLength() - 1);
            byte[] startBytes = pointables[1].getByteArray();
            int offset = pointables[1].getStartOffset();

            int subStart;

            // strange SQL index convention
            subStart = ATypeHierarchy.getIntegerValue(AsterixBuiltinFunctions.SUBBINARY_FROM.getName(), 1, startBytes,
                    offset) - 1;

            int totalLength = byteArrayPointable.getContentLength();
            int subLength = getSubLength(tuple);

            if (subStart < 0) {
                subStart = 0;
            }

            if (subStart >= totalLength || subLength < 0) {
                subLength = 0;
            } else if (subLength > totalLength // for the IntMax case
                    || subStart + subLength > totalLength) {
                subLength = totalLength - subStart;
            }

            dataOutput.write(ATypeTag.BINARY.serialize());
            int metaLength = VarLenIntEncoderDecoder.encode(subLength, metaBuffer, 0);
            dataOutput.write(metaBuffer, 0, metaLength);
            dataOutput.write(byteArrayPointable.getByteArray(), byteArrayPointable.getContentStartOffset() + subStart,
                    subLength);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        result.set(resultStorage);
    }

    protected abstract int getSubLength(IFrameTupleReference tuple) throws HyracksDataException;
}
