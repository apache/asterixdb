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
package org.apache.asterix.dataflow.data.nontagged.serde;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Set;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.junit.Test;
import org.reflections.Reflections;

public class SimpleSerializerDeserializerTest {

    @SuppressWarnings("rawtypes")
    @Test
    public void test() {
        Reflections reflections = new Reflections("org.apache.asterix.dataflow.data.nontagged.serde");
        Set<Class<? extends ISerializerDeserializer>> allClasses =
                reflections.getSubTypesOf(ISerializerDeserializer.class);

        for (Class<? extends ISerializerDeserializer> cl : allClasses) {
            String className = cl.getName();
            if (className.endsWith("ARecordSerializerDeserializer")
                    || className.endsWith("AUnorderedListSerializerDeserializer")
                    || className.endsWith("AOrderedListSerializerDeserializer")
                    || className.endsWith("AStringSerializerDeserializer")) {
                // Serializer/Deserializer for complex types can have (immutable) states.
                continue;
            }

            // Verifies the class does not have non-static fields.
            for (Field field : cl.getDeclaredFields()) {
                if (!java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                    throw new IllegalStateException(
                            "The serializer/deserializer " + cl.getName() + " is not stateless!");
                }
            }

            // Verifies the class follows the singleton pattern.
            for (Constructor constructor : cl.getDeclaredConstructors()) {
                if (!java.lang.reflect.Modifier.isPrivate(constructor.getModifiers())) {
                    throw new IllegalStateException("The serializer/deserializer " + cl.getName()
                            + " is not implemented as a singleton class!");
                }
            }
        }
    }
}
