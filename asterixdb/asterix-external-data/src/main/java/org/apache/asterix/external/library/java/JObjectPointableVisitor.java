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
package org.apache.asterix.external.library.java;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.external.api.IJListAccessor;
import org.apache.asterix.external.api.IJObject;
import org.apache.asterix.external.api.IJObjectAccessor;
import org.apache.asterix.external.api.IJRecordAccessor;
import org.apache.asterix.external.library.TypeInfo;
import org.apache.asterix.external.library.java.JObjectAccessors.JListAccessor;
import org.apache.asterix.external.library.java.JObjectAccessors.JRecordAccessor;
import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.visitor.IVisitablePointableVisitor;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class JObjectPointableVisitor implements IVisitablePointableVisitor<IJObject, TypeInfo> {

    private final Map<ATypeTag, IJObjectAccessor> flatJObjectAccessors = new HashMap<ATypeTag, IJObjectAccessor>();
    private final Map<IVisitablePointable, IJRecordAccessor> raccessorToJObject =
            new HashMap<IVisitablePointable, IJRecordAccessor>();
    private final Map<IVisitablePointable, IJListAccessor> laccessorToPrinter =
            new HashMap<IVisitablePointable, IJListAccessor>();

    @Override
    public IJObject visit(AListVisitablePointable accessor, TypeInfo arg) throws HyracksDataException {
        IJObject result = null;
        IJListAccessor jListAccessor = laccessorToPrinter.get(accessor);
        if (jListAccessor == null) {
            jListAccessor = new JListAccessor(arg.getObjectPool());
            laccessorToPrinter.put(accessor, jListAccessor);
        }
        result = jListAccessor.access(accessor, arg.getObjectPool(), arg.getAtype(), this);
        return result;
    }

    @Override
    public IJObject visit(ARecordVisitablePointable accessor, TypeInfo arg) throws HyracksDataException {
        IJObject result = null;
        IJRecordAccessor jRecordAccessor = raccessorToJObject.get(accessor);
        if (jRecordAccessor == null) {
            jRecordAccessor = new JRecordAccessor(accessor.getInputRecordType(), arg.getObjectPool());
            raccessorToJObject.put(accessor, jRecordAccessor);
        }
        result = jRecordAccessor.access(accessor, arg.getObjectPool(), (ARecordType) arg.getAtype(), this);
        return result;
    }

    @Override
    public IJObject visit(AFlatValuePointable accessor, TypeInfo arg) throws HyracksDataException {
        ATypeTag typeTag = arg.getTypeTag();
        IJObject result = null;
        IJObjectAccessor jObjectAccessor = flatJObjectAccessors.get(typeTag);
        if (jObjectAccessor == null) {
            jObjectAccessor = JObjectAccessors.createFlatJObjectAccessor(typeTag);
            flatJObjectAccessors.put(typeTag, jObjectAccessor);
        }

        result = jObjectAccessor.access(accessor, arg.getObjectPool());
        return result;
    }

}
