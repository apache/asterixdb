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
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder;

public class SubBinaryFromToDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SubBinaryFromToDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.SUBBINARY_FROM_TO;
    }

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {
            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                return new AbstractSubBinaryCopyEvaluator(output, args, getIdentifier().getName()) {
                    @Override
                    protected int getSubLength(IFrameTupleReference tuple) throws AlgebricksException {
                        ATypeTag tagSubLength = evaluateTuple(tuple, 2);
                        int subLength = 0;
                        try {
                            subLength = ATypeHierarchy.getIntegerValue(storages[2].getByteArray(), 0);
                        } catch (HyracksDataException e) {
                            throw new AlgebricksException(e);
                        }

                        return subLength;
                    }
                };
            }
        };
    }

    static abstract class AbstractSubBinaryCopyEvaluator extends AbstractCopyEvaluator {
        public AbstractSubBinaryCopyEvaluator(IDataOutputProvider output,
                ICopyEvaluatorFactory[] copyEvaluatorFactories, String functionName) throws AlgebricksException {
            super(output, copyEvaluatorFactories);
            this.functionName = functionName;
        }

        private ByteArrayPointable byteArrayPointable = new ByteArrayPointable();
        private byte[] metaBuffer = new byte[5];
        protected final String functionName;

        static final ATypeTag[] EXPECTED_INPUT_TAGS = { ATypeTag.BINARY, ATypeTag.INT32 };

        @Override
        public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {

            ATypeTag argTag0 = evaluateTuple(tuple, 0);
            ATypeTag argTag1 = evaluateTuple(tuple, 1);

            try {
                if (serializeNullIfAnyNull(argTag0, argTag1)) {
                    return;
                }
                checkTypeMachingThrowsIfNot(functionName, EXPECTED_INPUT_TAGS, argTag0, argTag1);

                byteArrayPointable.set(storages[0].getByteArray(), 1, storages[0].getLength() - 1);
                byte[] startBytes = storages[1].getByteArray();

                int subStart = 0;

                // strange SQL index convention
                subStart = ATypeHierarchy.getIntegerValue(startBytes, 0) - 1;

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
                dataOutput.write(byteArrayPointable.getByteArray(),
                        byteArrayPointable.getContentStartOffset() + subStart, subLength);
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
            } catch (IOException e) {
                throw new AlgebricksException(e);
            }
        }

        protected abstract int getSubLength(IFrameTupleReference tuple) throws AlgebricksException;
    }
}
