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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.AsterixListAccessor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder;

public class BinaryConcatDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new BinaryConcatDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.BINARY_CONCAT;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IHyracksTaskContext ctx) throws AlgebricksException {
                return new AbstractBinaryScalarEvaluator(ctx, args) {

                    private final AsterixListAccessor listAccessor = new AsterixListAccessor();
                    private final byte[] metaBuffer = new byte[5];

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
                        resultStorage.reset();
                        ATypeTag typeTag = evaluateTuple(tuple, 0);
                        if (typeTag != ATypeTag.UNORDEREDLIST && typeTag != ATypeTag.ORDEREDLIST) {
                            throw new AlgebricksException(getIdentifier().getName()
                                    + ": expects input type ORDEREDLIST/UNORDEREDLIST, but got " + typeTag);
                        }
                        try {
                            byte[] data = pointables[0].getByteArray();
                            int offset = pointables[0].getStartOffset();

                            listAccessor.reset(data, offset);
                            int concatLength = 0;
                            for (int i = 0; i < listAccessor.size(); i++) {
                                int itemOffset = listAccessor.getItemOffset(i);
                                ATypeTag itemType = listAccessor.getItemType(itemOffset);
                                if (itemType != ATypeTag.BINARY) {
                                    if (serializeNullIfAnyNull(itemType)) {
                                        result.set(resultStorage);
                                        return;
                                    }
                                    throw new AlgebricksException(getIdentifier().getName()
                                            + ": expects type STRING/NULL for the list item but got " + itemType);
                                }
                                concatLength += ByteArrayPointable.getContentLength(data, itemOffset);
                            }
                            dataOutput.writeByte(ATypeTag.SERIALIZED_BINARY_TYPE_TAG);
                            int metaLen = VarLenIntEncoderDecoder.encode(concatLength, metaBuffer, 0);
                            dataOutput.write(metaBuffer, 0, metaLen);

                            for (int i = 0; i < listAccessor.size(); i++) {
                                int itemOffset = listAccessor.getItemOffset(i);
                                int length = ByteArrayPointable.getContentLength(data, itemOffset);
                                dataOutput.write(data,
                                        itemOffset + ByteArrayPointable.getNumberBytesToStoreMeta(length), length);
                            }
                        } catch (HyracksDataException e) {
                            throw new AlgebricksException(e);
                        } catch (IOException e) {
                            throw new AlgebricksException(e);
                        } catch (AsterixException e) {
                            throw new AlgebricksException(e);
                        }
                        result.set(resultStorage);
                    }
                };

            }
        };

    }
}
