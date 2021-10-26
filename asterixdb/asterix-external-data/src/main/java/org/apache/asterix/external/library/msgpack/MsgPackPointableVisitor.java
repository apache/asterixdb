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

package org.apache.asterix.external.library.msgpack;

import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.external.library.PyTypeInfo;
import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.visitor.IVisitablePointableVisitor;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class MsgPackPointableVisitor implements IVisitablePointableVisitor<Void, PyTypeInfo> {

    Map<DataOutput, Map<IAType, PyTypeInfo>> typeInfoMap = new HashMap<>();

    public Void visit(AListVisitablePointable accessor, PyTypeInfo arg) throws HyracksDataException {
        MsgPackAccessors.MsgPackListAccessor.access(accessor, arg, this);
        return null;
    }

    @Override
    public Void visit(ARecordVisitablePointable accessor, PyTypeInfo arg) throws HyracksDataException {
        MsgPackAccessors.MsgPackRecordAccessor.access(accessor, arg, this);
        return null;
    }

    @Override
    public Void visit(AFlatValuePointable accessor, PyTypeInfo arg) throws HyracksDataException {
        try {
            MsgPackAccessors.createFlatMsgPackAccessor(arg.getType().getTypeTag()).apply(accessor, arg.getDataOutput());
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        return null;
    }

    public PyTypeInfo getTypeInfo(IAType type, DataOutput out) {
        PyTypeInfo tInfo = null;
        Map<IAType, PyTypeInfo> type2TypeInfo = typeInfoMap.get(out);
        if (type2TypeInfo == null) {
            type2TypeInfo = new HashMap<>();
            tInfo = new PyTypeInfo(type, out);
            type2TypeInfo.put(type, tInfo);
            typeInfoMap.put(out, type2TypeInfo);
        }
        tInfo = tInfo == null ? type2TypeInfo.get(type) : tInfo;
        if (tInfo == null) {
            tInfo = new PyTypeInfo(type, out);
            type2TypeInfo.put(type, tInfo);
        }
        return tInfo;
    }

}
