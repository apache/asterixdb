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
package org.apache.asterix.object.base;

import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.asterix.om.types.ATypeTag;

/**
 * An interface representing an adm node
 */
public interface IAdmNode extends Serializable {

    /**
     * @return true if the object is a value, false if the object is a container
     */
    default boolean isValueNode() {
        switch (getType()) {
            case ARRAY:
            case OBJECT:
            case MULTISET:
                return false;
            default:
                return true;
        }
    }

    /**
     * @return true if the object is an array, false otherwise
     */
    default boolean isArray() {
        return getType() == ATypeTag.ARRAY;
    }

    /**
     * @return true if the object is an adm object, false otherwise
     */
    default boolean isObject() {
        return getType() == ATypeTag.OBJECT;
    }

    /**
     * @return the type tag of the object
     */
    ATypeTag getType();

    /**
     * reset the node
     */
    void reset();

    /**
     * Serialize the field with a type tag
     *
     * @param dataOutput
     * @throws IOException
     */
    default void serialize(DataOutput dataOutput) throws IOException {
        dataOutput.writeByte(getType().serialize());
        serializeValue(dataOutput);
    }

    /**
     * Serialize the value without a type tag
     *
     * @param dataOutput
     * @throws IOException
     */
    void serializeValue(DataOutput dataOutput) throws IOException;
}
