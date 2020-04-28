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
package org.apache.asterix.external.library.java.base;

import static org.apache.asterix.om.types.AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE;
import static org.apache.asterix.om.types.AUnorderedListType.FULLY_OPEN_UNORDEREDLIST_TYPE;
import static org.apache.asterix.om.utils.RecordUtil.FULLY_OPEN_RECORD_TYPE;

import java.io.DataOutput;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.external.api.IJObject;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multiset;

public abstract class JObject<T> implements IJObject<T> {

    private static final Map<Class, IAType> typeConv = new ImmutableMap.Builder<Class, IAType>()
            .put(HashMap.class, FULLY_OPEN_RECORD_TYPE).put(Byte.class, BuiltinType.AINT8)
            .put(Short.class, BuiltinType.AINT16).put(Integer.class, BuiltinType.AINT32)
            .put(Long.class, BuiltinType.AINT64).put(Float.class, BuiltinType.AFLOAT)
            .put(Double.class, BuiltinType.ADOUBLE).put(LocalTime.class, BuiltinType.ATIME)
            .put(LocalDate.class, BuiltinType.ADATE).put(LocalDateTime.class, BuiltinType.ADATETIME)
            .put(Duration.class, BuiltinType.ADURATION).put(List.class, FULL_OPEN_ORDEREDLIST_TYPE)
            .put(String.class, BuiltinType.ASTRING).put(Multiset.class, FULLY_OPEN_UNORDEREDLIST_TYPE).build();
    protected IAObject value;
    protected byte[] bytes;
    protected IObjectPool<IJObject, Class> pool;

    protected JObject() {

    }

    public static IAType convertType(Class clazz) {
        return typeConv.get(clazz);
    }

    protected JObject(IAObject value) {
        this.value = value;
    }

    @Override
    public IAObject getIAObject() {
        return value;
    }

    public void serializeTypeTag(boolean writeTypeTag, DataOutput dataOutput, ATypeTag typeTag)
            throws HyracksDataException {
        if (writeTypeTag) {
            try {
                dataOutput.writeByte(typeTag.serialize());
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }
    }
}
