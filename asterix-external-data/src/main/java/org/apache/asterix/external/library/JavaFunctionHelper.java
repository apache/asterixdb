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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.external.api.IJObject;
import org.apache.asterix.external.library.java.JObjectPointableVisitor;
import org.apache.asterix.external.library.java.JObjects.JNull;
import org.apache.asterix.external.library.java.JTypeTag;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.api.IValueReference;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class JavaFunctionHelper implements IFunctionHelper {

    private final IExternalFunctionInfo finfo;
    private final IDataOutputProvider outputProvider;
    private final IJObject[] arguments;
    private IJObject resultHolder;
    private final IObjectPool<IJObject, IAType> objectPool = new ListObjectPool<IJObject, IAType>(
            JTypeObjectFactory.INSTANCE);
    private final JObjectPointableVisitor pointableVisitor;
    private final PointableAllocator pointableAllocator;
    private final Map<Integer, TypeInfo> poolTypeInfo;

    private boolean isValidResult = false;

    public JavaFunctionHelper(IExternalFunctionInfo finfo, IDataOutputProvider outputProvider)
            throws AlgebricksException {
        this.finfo = finfo;
        this.outputProvider = outputProvider;
        this.pointableVisitor = new JObjectPointableVisitor();
        this.pointableAllocator = new PointableAllocator();
        this.arguments = new IJObject[finfo.getParamList().size()];
        int index = 0;
        for (IAType param : finfo.getParamList()) {
            this.arguments[index++] = objectPool.allocate(param);
        }
        this.resultHolder = objectPool.allocate(finfo.getReturnType());
        this.poolTypeInfo = new HashMap<Integer, TypeInfo>();

    }

    @Override
    public IJObject getArgument(int index) {
        return arguments[index];
    }

    @Override
    public void setResult(IJObject result) throws IOException, AsterixException {
        if (result == null) {
            JNull.INSTANCE.serialize(outputProvider.getDataOutput(), true);
            isValidResult = false;
        } else {
            try {
                isValidResult = true;
                result.serialize(outputProvider.getDataOutput(), true);
                result.reset();
            } catch (IOException | AlgebricksException e) {
                throw new HyracksDataException(e);
            }
        }
    }

    /**
     * Gets the value of the result flag
     *
     * @return
     *    boolean True is the setResult is called and result is not null
     */
    @Override
    public boolean isValidResult() {
        return this.isValidResult;
    }

    public void setArgument(int index, IValueReference valueReference) throws IOException, AsterixException {
        IVisitablePointable pointable = null;
        IJObject jObject = null;
        IAType type = finfo.getParamList().get(index);
        switch (type.getTypeTag()) {
            case RECORD:
                pointable = pointableAllocator.allocateRecordValue(type);
                pointable.set(valueReference);
                jObject = pointableVisitor.visit((ARecordVisitablePointable) pointable, getTypeInfo(index, type));
                break;
            case ORDEREDLIST:
            case UNORDEREDLIST:
                pointable = pointableAllocator.allocateListValue(type);
                pointable.set(valueReference);
                jObject = pointableVisitor.visit((AListVisitablePointable) pointable, getTypeInfo(index, type));
                break;
            case ANY:
                throw new IllegalStateException("Cannot handle a function argument of type " + type.getTypeTag());
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
    public IJObject getObject(JTypeTag jtypeTag) {
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
                    throw new NotImplementedException("Object of type " + jtypeTag.name() + " not supported.");
                } catch (IllegalStateException e) {
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

}