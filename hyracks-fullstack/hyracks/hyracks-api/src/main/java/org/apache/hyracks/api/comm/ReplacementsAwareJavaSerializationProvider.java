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
package org.apache.hyracks.api.comm;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hyracks.util.ThrowingIOFunction;

public class ReplacementsAwareJavaSerializationProvider implements IJavaSerializationProvider {
    public static final ReplacementsAwareJavaSerializationProvider INSTANCE =
            new ReplacementsAwareJavaSerializationProvider();
    private static final Map<Class<?>, ThrowingIOFunction<Object, Object>> replacements = new ConcurrentHashMap<>();

    private ReplacementsAwareJavaSerializationProvider() {
    }

    @Override
    public ObjectOutputStream newObjectOutputStream(OutputStream out) throws IOException {
        return new ReplacementsAwareObjectOutputStream(out);
    }

    public Map<Class<?>, ThrowingIOFunction<Object, Object>> getReplacements() {
        return Collections.unmodifiableMap(replacements);
    }

    public void registerReplacement(Class<?> clazz, ThrowingIOFunction<Object, Object> replacementFunction) {
        replacements.put(clazz, replacementFunction);
    }

    private static class ReplacementsAwareObjectOutputStream extends ObjectOutputStream {
        public ReplacementsAwareObjectOutputStream(OutputStream out) throws IOException {
            super(out);
            enableReplaceObject(true);
        }

        @Override
        protected Object replaceObject(Object object) throws IOException {
            Class<?> clazz = object.getClass();
            if (clazz.isSynthetic()) {
                return super.replaceObject(object);
            }

            // try exact match first (fast path)
            ThrowingIOFunction<Object, Object> replacementFunction = replacements.get(clazz);
            if (replacementFunction != null) {
                return replacementFunction.process(object);
            }

            // fallback: match by assignability (handles subclasses / interfaces)
            for (Map.Entry<Class<?>, ThrowingIOFunction<Object, Object>> e : replacements.entrySet()) {
                if (e.getKey().isInstance(object)) {
                    INSTANCE.registerReplacement(clazz, e.getValue());
                    return e.getValue().process(object);
                }
            }

            INSTANCE.registerReplacement(clazz, super::replaceObject);
            return super.replaceObject(object);
        }
    }
}
