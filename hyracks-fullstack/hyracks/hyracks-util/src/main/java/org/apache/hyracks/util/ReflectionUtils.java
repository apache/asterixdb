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
package org.apache.hyracks.util;

import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ReflectionUtils {
    private static final Logger LOGGER = LogManager.getLogger();

    private ReflectionUtils() {
    }

    public static <T> T createInstance(Class<? extends T> klass) {
        T instance = null;
        try {
            instance = klass.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        return instance;
    }

    public static Field getAccessibleField(Class<?> clazz, String fieldName) throws NoSuchFieldException {
        Field f = clazz.getDeclaredField(fieldName);
        f.setAccessible(true);
        return f;
    }

    public static Field getAccessibleField(Object obj, String fieldName) throws NoSuchFieldException {
        Class<?> cl = obj.getClass();
        while (true) {
            Field f = null;
            try {
                f = getAccessibleField(cl, fieldName);
                return f;
            } catch (NoSuchFieldException e) {
                cl = cl.getSuperclass();
                if (cl == null) {
                    throw new NoSuchFieldException(
                            "field: '" + fieldName + "' not found in (hierarchy of) " + obj.getClass());
                }
            }
        }
    }

    public static Object readField(Object obj, String fieldName) throws IOException {
        try {
            return readField(obj, getAccessibleField(obj, fieldName));
        } catch (NoSuchFieldException e) {
            throw new IOException(e);
        }
    }

    public static Object readField(Object obj, Field f) throws IOException {
        Class<?> objClass = obj.getClass();
        LOGGER.debug("reading field '{}' on object of type {}", f::getName, objClass::toString);
        try {
            return f.get(obj);
        } catch (IllegalAccessException e) {
            LOGGER.warn("exception reading field '{}' on object of type {}", f.getName(), objClass, e);
            throw new IOException(e);
        }
    }

    public static void writeField(Object obj, String fieldName, Object newValue) throws IOException {
        try {
            writeField(obj, getAccessibleField(obj, fieldName), newValue);
        } catch (NoSuchFieldException e) {
            throw new IOException(e);
        }
    }

    public static void writeField(Object obj, Field f, Object newValue) throws IOException {
        Class<?> objClass = obj.getClass();
        LOGGER.debug("updating field '{}' on object of type {} to {}", f::getName, objClass::toString,
                newValue::toString);
        try {
            f.set(obj, newValue);
        } catch (IllegalAccessException e) {
            LOGGER.warn("exception updating field '{}' object of type {} to {}", f.getName(), objClass, newValue, e);
            throw new IOException(e);
        }
    }
}
