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
package org.apache.asterix.runtime.evaluators.functions;

import static org.apache.asterix.om.types.ATypeTag.BIGINT;
import static org.apache.asterix.om.types.ATypeTag.MISSING;
import static org.apache.asterix.om.types.ATypeTag.NULL;
import static org.apache.asterix.om.types.ATypeTag.VALUE_TYPE_MAPPING;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.string.UTF8StringUtil;

@MissingNullInOutFunction
public class CodePointToStringDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = CodePointToStringDescriptor::new;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {

                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final DataOutput out = resultStorage.getDataOutput();
                    private final IScalarEvaluatorFactory listEvalFactory = args[0];
                    private final IPointable inputArgList = new VoidPointable();
                    private final IScalarEvaluator evalList = listEvalFactory.createScalarEvaluator(ctx);
                    private final byte[] currentUTF8 = new byte[6];
                    private final byte[] tempStoreForLength = new byte[5];
                    private final FunctionIdentifier fid = getIdentifier();
                    private final char[] tempCharPair = new char[2];

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        try {
                            resultStorage.reset();
                            Arrays.fill(tempStoreForLength, (byte) 0);
                            Arrays.fill(currentUTF8, (byte) 0);
                            evalList.evaluate(tuple, inputArgList);

                            if (PointableHelper.checkAndSetMissingOrNull(result, inputArgList)) {
                                return;
                            }

                            byte[] serOrderedList = inputArgList.getByteArray();
                            int offset = inputArgList.getStartOffset();
                            if (serOrderedList[offset] != ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG) {
                                PointableHelper.setNull(result);
                                ExceptionUtil.warnTypeMismatch(ctx, sourceLoc, fid, serOrderedList[offset], 0,
                                        ATypeTag.ARRAY);
                                return;
                            }
                            int itemTagPosition = offset + 1;
                            ATypeTag itemTag = VALUE_TYPE_MAPPING[serOrderedList[itemTagPosition]];
                            boolean isItemTagged = itemTag == ATypeTag.ANY;
                            int size = AOrderedListSerializerDeserializer.getNumberOfItems(serOrderedList, offset);
                            // calculate length first
                            int utf_8_len = 0;
                            boolean returnNull = false;
                            ATypeTag invalidTag = null;
                            for (int i = 0; i < size; i++) {
                                int itemOffset =
                                        AOrderedListSerializerDeserializer.getItemOffset(serOrderedList, offset, i);
                                if (isItemTagged) {
                                    itemTag = VALUE_TYPE_MAPPING[serOrderedList[itemOffset]];
                                    itemTagPosition = itemOffset;
                                    itemOffset++;
                                }
                                if (itemTag == MISSING) {
                                    PointableHelper.setMissing(result);
                                    return;
                                }
                                if (itemTag == NULL) {
                                    returnNull = true;
                                    invalidTag = null;
                                } else if (!returnNull && !ATypeHierarchy.canPromote(itemTag, BIGINT)) {
                                    returnNull = true;
                                    invalidTag = itemTag;
                                }
                                if (!returnNull) {
                                    int codePoint = ATypeHierarchy.getIntegerValueWithDifferentTypeTagPosition(
                                            fid.getName(), 0, serOrderedList, itemOffset, itemTagPosition);
                                    utf_8_len += UTF8StringUtil.codePointToUTF8(codePoint, tempCharPair, currentUTF8);
                                }
                            }
                            if (returnNull) {
                                PointableHelper.setNull(result);
                                if (invalidTag != null) {
                                    ExceptionUtil.warnUnsupportedType(ctx, sourceLoc, fid.getName(), invalidTag);
                                }
                                return;
                            }
                            out.writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                            UTF8StringUtil.writeUTF8Length(utf_8_len, tempStoreForLength, out);
                            for (int i = 0; i < size; i++) {
                                int itemOffset =
                                        AOrderedListSerializerDeserializer.getItemOffset(serOrderedList, offset, i);
                                if (isItemTagged) {
                                    itemTagPosition = itemOffset;
                                    itemOffset++;
                                }
                                int codePoint = ATypeHierarchy.getIntegerValueWithDifferentTypeTagPosition(
                                        fid.getName(), 0, serOrderedList, itemOffset, itemTagPosition);
                                utf_8_len = UTF8StringUtil.codePointToUTF8(codePoint, tempCharPair, currentUTF8);
                                for (int j = 0; j < utf_8_len; j++) {
                                    out.writeByte(currentUTF8[j]);
                                }
                            }
                            result.set(resultStorage);
                        } catch (IOException e) {
                            throw HyracksDataException.create(e);
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.CODEPOINT_TO_STRING;
    }
}
