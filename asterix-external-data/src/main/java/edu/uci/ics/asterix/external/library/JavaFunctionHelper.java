/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.external.library;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.external.library.java.IJObject;
import edu.uci.ics.asterix.external.library.java.JObjectPointableVisitor;
import edu.uci.ics.asterix.external.library.java.JTypeTag;
import edu.uci.ics.asterix.om.functions.IExternalFunctionInfo;
import edu.uci.ics.asterix.om.pointables.AFlatValuePointable;
import edu.uci.ics.asterix.om.pointables.AListPointable;
import edu.uci.ics.asterix.om.pointables.ARecordPointable;
import edu.uci.ics.asterix.om.pointables.PointableAllocator;
import edu.uci.ics.asterix.om.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.container.IObjectPool;
import edu.uci.ics.asterix.om.util.container.ListObjectPool;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.api.IValueReference;

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
        try {
            result.serialize(outputProvider.getDataOutput(), true);
            result.reset();
        } catch (IOException  | AlgebricksException e) {
            throw new HyracksDataException(e);
        }
    }

    public void setArgument(int index, IValueReference valueReference) throws IOException, AsterixException {
        IVisitablePointable pointable = null;
        IJObject jObject = null;
        IAType type = finfo.getParamList().get(index);
        switch (type.getTypeTag()) {
            case RECORD:
                pointable = pointableAllocator.allocateRecordValue(type);
                pointable.set(valueReference);
                jObject = pointableVisitor.visit((ARecordPointable) pointable, getTypeInfo(index, type));
                break;
            case ORDEREDLIST:
            case UNORDEREDLIST:
                pointable = pointableAllocator.allocateListValue(type);
                pointable.set(valueReference);
                jObject = pointableVisitor.visit((AListPointable) pointable, getTypeInfo(index, type));
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
        }
        return retValue;
    }

    @Override
    public void reset() {
        pointableAllocator.reset();
        objectPool.reset();
    }

}