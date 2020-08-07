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
package org.apache.asterix.external.library;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.external.api.IJObject;
import org.apache.asterix.external.library.java.JObjectPointableVisitor;
import org.apache.asterix.external.library.java.JTypeTag;
import org.apache.asterix.external.library.java.base.JNull;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.api.IValueReference;

public class JavaFunctionHelper implements IFunctionHelper {

    private final IExternalFunctionInfo finfo;
    private final IDataOutputProvider outputProvider;
    private final IJObject[] arguments;
    private IJObject resultHolder;
    private final IObjectPool<IJObject, IAType> objectPool = new ListObjectPool<>(JTypeObjectFactory.INSTANCE);
    private final JObjectPointableVisitor pointableVisitor;
    private final PointableAllocator pointableAllocator;
    private final Map<Integer, TypeInfo> poolTypeInfo;
    private final Map<String, String> parameters;
    private final IAType[] argTypes;

    private boolean isValidResult = false;

    JavaFunctionHelper(IExternalFunctionInfo finfo, IAType[] argTypes, IDataOutputProvider outputProvider) {
        this.finfo = finfo;
        this.outputProvider = outputProvider;
        this.pointableVisitor = new JObjectPointableVisitor();
        this.pointableAllocator = new PointableAllocator();
        this.arguments = new IJObject[finfo.getParameterTypes().size()];
        int index = 0;
        for (IAType param : finfo.getParameterTypes()) {
            this.arguments[index++] = objectPool.allocate(param);
        }
        this.resultHolder = objectPool.allocate(finfo.getReturnType());
        this.poolTypeInfo = new HashMap<>();
        this.parameters = finfo.getResources();
        this.argTypes = argTypes;

    }

    @Override
    public IJObject getArgument(int index) {
        return arguments[index];
    }

    @Override
    public void setResult(IJObject result) throws HyracksDataException {
        if (result == null || checkInvalidReturnValueType(result, finfo.getReturnType())) {
            isValidResult = false;
        } else {
            isValidResult = true;
            result.serialize(outputProvider.getDataOutput(), true);
            result.reset();
        }
    }

    private boolean checkInvalidReturnValueType(IJObject result, IAType expectedType) {
        if (expectedType.equals(BuiltinType.ANY)) {
            return false;
        }
        return !expectedType.deepEqual(result.getIAType());
    }

    /**
     * Gets the value of the result flag
     *
     * @return
     *         boolean True is the setResult is called and result is not null
     */
    @Override
    public boolean isValidResult() {
        return this.isValidResult;
    }

    void setArgument(int index, IValueReference valueReference) throws IOException {
        IVisitablePointable pointable;
        IJObject jObject;
        IAType type = argTypes[index];
        switch (type.getTypeTag()) {
            case OBJECT:
                pointable = pointableAllocator.allocateRecordValue(type);
                pointable.set(valueReference);
                jObject = pointableVisitor.visit((ARecordVisitablePointable) pointable, getTypeInfo(index, type));
                break;
            case ARRAY:
            case MULTISET:
                pointable = pointableAllocator.allocateListValue(type);
                pointable.set(valueReference);
                jObject = pointableVisitor.visit((AListVisitablePointable) pointable, getTypeInfo(index, type));
                break;
            case ANY:
                ATypeTag rtTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                        .deserialize(valueReference.getByteArray()[valueReference.getStartOffset()]);
                IAType rtType = TypeTagUtil.getBuiltinTypeByTag(rtTypeTag);
                switch (rtTypeTag) {
                    case OBJECT:
                        pointable = pointableAllocator.allocateRecordValue(rtType);
                        pointable.set(valueReference);
                        jObject = pointableVisitor.visit((ARecordVisitablePointable) pointable,
                                getTypeInfo(index, rtType));
                        break;
                    case ARRAY:
                    case MULTISET:
                        pointable = pointableAllocator.allocateListValue(rtType);
                        pointable.set(valueReference);
                        jObject =
                                pointableVisitor.visit((AListVisitablePointable) pointable, getTypeInfo(index, rtType));
                        break;
                    default:
                        pointable = pointableAllocator.allocateFieldValue(rtType);
                        pointable.set(valueReference);
                        jObject = pointableVisitor.visit((AFlatValuePointable) pointable, rtTypeTag,
                                getTypeInfo(index, rtType));
                        break;
                }
                break;
            default:
                pointable = pointableAllocator.allocateFieldValue(type);
                pointable.set(valueReference);
                jObject = pointableVisitor.visit((AFlatValuePointable) pointable, getTypeInfo(index, type));
                break;
        }
        arguments[index] = jObject;
    }

    private TypeInfo getTypeInfo(int index, IAType type) {
        TypeInfo typeInfo = poolTypeInfo.get(index);
        if (typeInfo == null) {
            typeInfo = new TypeInfo(objectPool, type, type.getTypeTag());
            poolTypeInfo.put(index, typeInfo);
        }
        return typeInfo;
    }

    @Override
    public IJObject getResultObject() {
        if (resultHolder == null) {
            resultHolder = objectPool.allocate(finfo.getReturnType());
        }
        return resultHolder;
    }

    @Override
    public IJObject getResultObject(IAType type) {
        if (resultHolder == null) {
            resultHolder = objectPool.allocate(type);
        }
        return resultHolder;
    }

    @Override
    public IJObject getObject(JTypeTag jtypeTag) throws RuntimeDataException {
        IJObject retValue = null;
        switch (jtypeTag) {
            case INT:
                retValue = objectPool.allocate(BuiltinType.AINT32);
                break;
            case STRING:
                retValue = objectPool.allocate(BuiltinType.ASTRING);
                break;
            case DOUBLE:
                retValue = objectPool.allocate(BuiltinType.ADOUBLE);
                break;
            case NULL:
                retValue = JNull.INSTANCE;
                break;
            default:
                try {
                    throw new RuntimeDataException(ErrorCode.LIBRARY_JAVA_FUNCTION_HELPER_OBJ_TYPE_NOT_SUPPORTED,
                            jtypeTag.name());
                } catch (IllegalStateException e) {
                    // Exception is not thrown
                    e.printStackTrace();
                }
                break;
        }
        return retValue;
    }

    @Override
    public void reset() {
        pointableAllocator.reset();
        objectPool.reset();
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    @Override
    public List<String> getExternalIdentifier() {
        return finfo.getExternalIdentifier();
    }

}
