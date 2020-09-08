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
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

@MissingNullInOutFunction
public final class DecodeDataverseNameDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = DecodeDataverseNameDescriptor::new;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new AbstractScalarEval(sourceLoc, getIdentifier()) {
                    private final IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);

                    private final VoidPointable arg0 = VoidPointable.FACTORY.createPointable();
                    private final UTF8StringPointable strPtr = new UTF8StringPointable();
                    private final List<String> dataverseNameParts = new ArrayList<>();

                    private final AOrderedListType listType = new AOrderedListType(BuiltinType.ASTRING, null);
                    private final OrderedListBuilder listBuilder = new OrderedListBuilder();
                    private final ArrayBackedValueStorage itemStorage = new ArrayBackedValueStorage();
                    private final DataOutput itemOut = itemStorage.getDataOutput();
                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final DataOutput resultOut = resultStorage.getDataOutput();

                    @SuppressWarnings("rawtypes")
                    private final ISerializerDeserializer stringSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);
                    private final AMutableString aString = new AMutableString("");

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        eval0.evaluate(tuple, arg0);
                        if (PointableHelper.checkAndSetMissingOrNull(result, arg0)) {
                            return;
                        }

                        byte[] bytes = arg0.getByteArray();
                        int offset = arg0.getStartOffset();
                        int len = arg0.getLength();

                        // Type check.
                        if (bytes[offset] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                            PointableHelper.setNull(result);
                            ExceptionUtil.warnTypeMismatch(ctx, srcLoc, getIdentifier(), bytes[offset], 0,
                                    ATypeTag.STRING);
                            return;
                        }

                        strPtr.set(bytes, offset + 1, len - 1);
                        String dataverseCanonicalName = strPtr.toString();

                        dataverseNameParts.clear();
                        DataverseName.getPartsFromCanonicalForm(dataverseCanonicalName, dataverseNameParts);

                        resultStorage.reset();
                        listBuilder.reset(listType);
                        for (String part : dataverseNameParts) {
                            itemStorage.reset();
                            aString.setValue(part);
                            stringSerde.serialize(aString, itemOut);
                            listBuilder.addItem(itemStorage);
                        }
                        listBuilder.write(resultOut, true);
                        result.set(resultStorage);
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.DECODE_DATAVERSE_NAME;
    }
}
