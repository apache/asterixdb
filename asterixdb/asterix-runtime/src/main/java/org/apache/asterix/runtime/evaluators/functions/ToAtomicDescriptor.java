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

import java.util.List;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

@MissingNullInOutFunction
public class ToAtomicDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ToAtomicDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            return FunctionTypeInferers.SET_ARGUMENT_TYPE;
        }
    };

    private static final long serialVersionUID = 1L;
    private IAType argType;

    @Override
    public void setImmutableStates(Object... states) {
        argType = (IAType) states[0];
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {

                return new IScalarEvaluator() {

                    private final IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private final IPointable arg = new VoidPointable();

                    private final PointableAllocator pAlloc = new PointableAllocator();

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable resultPointable)
                            throws HyracksDataException {
                        eval0.evaluate(tuple, arg);

                        if (PointableHelper.checkAndSetMissingOrNull(resultPointable, arg)) {
                            return;
                        }

                        IValueReference itemPtr = arg;
                        IAType itemTypeInferred = argType;

                        for (;;) {
                            byte[] itemData = itemPtr.getByteArray();
                            int itemOffset = itemPtr.getStartOffset();
                            ATypeTag typeTag = ATypeTag.VALUE_TYPE_MAPPING[itemData[itemOffset]];
                            switch (typeTag) {
                                case ARRAY:
                                case MULTISET:
                                    AListVisitablePointable listPointable =
                                            (AListVisitablePointable) allocatePointable(itemTypeInferred, typeTag);
                                    listPointable.set(itemPtr);
                                    List<IVisitablePointable> listItems = listPointable.getItems();
                                    if (listItems.size() != 1) {
                                        PointableHelper.setNull(resultPointable);
                                        return;
                                    }
                                    itemPtr = listItems.get(0);
                                    itemTypeInferred = getListItemType(itemTypeInferred);
                                    break;
                                case OBJECT:
                                    ARecordType recType = asRecordType(itemTypeInferred);
                                    ARecordVisitablePointable recPointable =
                                            (ARecordVisitablePointable) allocatePointable(itemTypeInferred, typeTag);
                                    recPointable.set(itemPtr);
                                    List<IVisitablePointable> recValues = recPointable.getFieldValues();
                                    if (recValues.size() != 1) {
                                        PointableHelper.setNull(resultPointable);
                                        return;
                                    }
                                    itemPtr = recValues.get(0);
                                    itemTypeInferred = recType.getFieldTypes().length == 1 ? recType.getFieldTypes()[0]
                                            : BuiltinType.ANY;
                                    break;
                                default:
                                    resultPointable.set(itemPtr);
                                    return;
                            }
                        }
                    }

                    private IVisitablePointable allocatePointable(IAType inferredType, ATypeTag actualTypeTag) {
                        if (inferredType.equals(BuiltinType.ANY)) {
                            return allocatePointableForAny(actualTypeTag);
                        }
                        return pAlloc.allocateFieldValue(inferredType);
                    }

                    private IVisitablePointable allocatePointableForAny(ATypeTag typeTag) {
                        switch (typeTag) {
                            case OBJECT:
                                return pAlloc.allocateFieldValue(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
                            case ARRAY:
                                return pAlloc.allocateFieldValue(DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE);
                            case MULTISET:
                                return pAlloc.allocateFieldValue(DefaultOpenFieldType.NESTED_OPEN_AUNORDERED_LIST_TYPE);
                            default:
                                return pAlloc.allocateFieldValue(null);
                        }
                    }

                    private ARecordType asRecordType(IAType inferredType) {
                        switch (inferredType.getTypeTag()) {
                            case OBJECT:
                                return (ARecordType) inferredType;
                            case UNION:
                                IAType innerType = ((AUnionType) inferredType).getActualType();
                                if (innerType.getTypeTag() == ATypeTag.OBJECT) {
                                    return (ARecordType) innerType;
                                }
                        }
                        return DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
                    }

                    private IAType getListItemType(IAType inferredType) {
                        switch (inferredType.getTypeTag()) {
                            case ARRAY:
                                return ((AOrderedListType) inferredType).getItemType();
                            case MULTISET:
                                return ((AUnorderedListType) inferredType).getItemType();
                            case UNION:
                                IAType innerType = ((AUnionType) inferredType).getActualType();
                                switch (innerType.getTypeTag()) {
                                    case ARRAY:
                                        return ((AOrderedListType) innerType).getItemType();
                                    case MULTISET:
                                        return ((AUnorderedListType) innerType).getItemType();
                                }
                        }
                        return BuiltinType.ANY;
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.TO_ATOMIC;
    }
}
