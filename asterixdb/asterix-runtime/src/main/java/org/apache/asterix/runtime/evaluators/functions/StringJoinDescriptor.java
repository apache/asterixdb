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

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
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
public class StringJoinDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = StringJoinDescriptor::new;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {
                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final ListAccessor listAccessor = new ListAccessor();
                    private final DataOutput out = resultStorage.getDataOutput();
                    private final IScalarEvaluatorFactory listEvalFactory = args[0];
                    private final IScalarEvaluatorFactory sepEvalFactory = args[1];
                    private final IPointable inputArgList = new VoidPointable();
                    private final IPointable inputArgSep = new VoidPointable();
                    private final IScalarEvaluator evalList = listEvalFactory.createScalarEvaluator(ctx);
                    private final IScalarEvaluator evalSep = sepEvalFactory.createScalarEvaluator(ctx);
                    private final byte[] tempLengthArray = new byte[5];
                    private final byte[] expectedTypeList =
                            { ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG, ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG };

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        evalList.evaluate(tuple, inputArgList);
                        evalSep.evaluate(tuple, inputArgSep);

                        if (PointableHelper.checkAndSetMissingOrNull(result, inputArgList, inputArgSep)) {
                            return;
                        }

                        byte[] listBytes = inputArgList.getByteArray();
                        int listOffset = inputArgList.getStartOffset();
                        if (listBytes[listOffset] != ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG
                                && listBytes[listOffset] != ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG) {
                            PointableHelper.setNull(result);
                            ExceptionUtil.warnTypeMismatch(ctx, sourceLoc, getIdentifier(), listBytes[listOffset], 0,
                                    expectedTypeList);
                            return;
                        }
                        byte[] sepBytes = inputArgSep.getByteArray();
                        int sepOffset = inputArgSep.getStartOffset();
                        if (sepBytes[sepOffset] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                            PointableHelper.setNull(result);
                            ExceptionUtil.warnTypeMismatch(ctx, sourceLoc, getIdentifier(), sepBytes[sepOffset], 1,
                                    ATypeTag.STRING);
                            return;
                        }
                        int sepLen = UTF8StringUtil.getUTFLength(sepBytes, sepOffset + 1);
                        int sepMetaLen = UTF8StringUtil.getNumBytesToStoreLength(sepLen);

                        listAccessor.reset(listBytes, listOffset);
                        try {
                            // calculate length first
                            int utf8Len = 0;
                            int size = listAccessor.size();
                            for (int i = 0; i < size; i++) {
                                int itemOffset = listAccessor.getItemOffset(i);
                                ATypeTag itemType = listAccessor.getItemType(itemOffset);
                                // Increase the offset by 1 if the give list has heterogeneous elements,
                                // since the item itself has a typetag.
                                if (listAccessor.itemsAreSelfDescribing()) {
                                    itemOffset += 1;
                                }
                                if (itemType != ATypeTag.STRING) {
                                    if (itemType == ATypeTag.MISSING) {
                                        PointableHelper.setMissing(result);
                                        return;
                                    }
                                    PointableHelper.setNull(result);
                                    if (itemType != ATypeTag.NULL) {
                                        // warn only if the call is: string_join([1,3], "/") where elements are non-null
                                        ExceptionUtil.warnUnsupportedType(ctx, sourceLoc, getIdentifier().getName(),
                                                itemType);
                                    }
                                    return;
                                }
                                int currentSize = UTF8StringUtil.getUTFLength(listBytes, itemOffset);
                                if (i != size - 1 && currentSize != 0) {
                                    utf8Len += sepLen;
                                }
                                utf8Len += currentSize;
                            }

                            out.writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                            int cbytes = UTF8StringUtil.encodeUTF8Length(utf8Len, tempLengthArray, 0);
                            out.write(tempLengthArray, 0, cbytes);
                            for (int i = 0; i < listAccessor.size(); i++) {
                                int itemOffset = listAccessor.getItemOffset(i);
                                if (listAccessor.itemsAreSelfDescribing()) {
                                    itemOffset += 1;
                                }
                                utf8Len = UTF8StringUtil.getUTFLength(listBytes, itemOffset);
                                out.write(listBytes, UTF8StringUtil.getNumBytesToStoreLength(utf8Len) + itemOffset,
                                        utf8Len);
                                for (int j = 0; j < sepLen; j++) {
                                    out.writeByte(sepBytes[sepOffset + 1 + sepMetaLen + j]);
                                }
                            }
                        } catch (IOException ex) {
                            throw HyracksDataException.create(ex);
                        }
                        result.set(resultStorage);
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.STRING_JOIN;
    }
}
