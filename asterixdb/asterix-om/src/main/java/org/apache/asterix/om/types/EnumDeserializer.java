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
package org.apache.asterix.om.types;

import java.lang.reflect.Array;

public class EnumDeserializer<E extends Enum<E> & IEnumSerializer> {

    public static final EnumDeserializer<ATypeTag> ATYPETAGDESERIALIZER =
            new EnumDeserializer<ATypeTag>(ATypeTag.class);

    private final E[] enumvalMap;

    @SuppressWarnings("unchecked")
    private EnumDeserializer(Class<E> enumClass) {
        enumvalMap = (E[]) Array.newInstance(enumClass, Byte.MAX_VALUE);
        for (E constant : enumClass.getEnumConstants()) {
            enumvalMap[constant.serialize()] = constant;
        }
    }

    public E deserialize(byte value) {
        return enumvalMap[value];
    }

}
