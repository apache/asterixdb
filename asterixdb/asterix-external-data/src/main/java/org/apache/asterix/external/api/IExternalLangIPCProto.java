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
package org.apache.asterix.external.api;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.ipc.MessageType;
import org.apache.asterix.external.library.msgpack.MsgPackPointableVisitor;
import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public interface IExternalLangIPCProto {
    static void visitValueRef(IAType type, DataOutput out, IValueReference valueReference,
            PointableAllocator pointableAllocator, MsgPackPointableVisitor pointableVisitor, boolean visitNull)
            throws IOException {
        IVisitablePointable pointable;
        switch (type.getTypeTag()) {
            case OBJECT:
                pointable = pointableAllocator.allocateRecordValue(type);
                pointable.set(valueReference);
                pointableVisitor.visit((ARecordVisitablePointable) pointable, pointableVisitor.getTypeInfo(type, out));
                break;
            case ARRAY:
            case MULTISET:
                pointable = pointableAllocator.allocateListValue(type);
                pointable.set(valueReference);
                pointableVisitor.visit((AListVisitablePointable) pointable, pointableVisitor.getTypeInfo(type, out));
                break;
            case ANY:
                ATypeTag rtTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                        .deserialize(valueReference.getByteArray()[valueReference.getStartOffset()]);
                IAType rtType = TypeTagUtil.getBuiltinTypeByTag(rtTypeTag);
                visitValueRef(rtType, out, valueReference, pointableAllocator, pointableVisitor, visitNull);
                break;
            case MISSING:
            case NULL:
                if (!visitNull) {
                    return;
                }
            default:
                pointable = pointableAllocator.allocateFieldValue(type);
                pointable.set(valueReference);
                pointableVisitor.visit((AFlatValuePointable) pointable, pointableVisitor.getTypeInfo(type, out));
                break;
        }
    }

    void start();

    void helo() throws IOException, AsterixException;

    long init(String module, String clazz, String fn) throws IOException, AsterixException;

    ByteBuffer call(long functionId, IAType[] argTypes, IValueReference[] argValues, boolean nullCall)
            throws IOException, AsterixException;

    ByteBuffer callMulti(long key, ArrayBackedValueStorage args, int numTuples) throws IOException, AsterixException;

    //For future use with interpreter reuse between jobs.
    void quit() throws HyracksDataException;

    void receiveMsg() throws IOException, AsterixException;

    void sendHeader(long key, int msgLen) throws IOException;

    void sendMsg(ArrayBackedValueStorage content) throws IOException;

    void sendMsg() throws IOException;

    MessageType getResponseType();

    long getRouteId();

    DataOutputStream getSockOut();
}
