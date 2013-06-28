/*
 * Copyright 2009-2013 by The Regents of the University of California
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
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hivesterix.serde.lazy.objectinspector.primitive;

import java.util.HashMap;

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

/**
 * PrimitiveObjectInspectorFactory is the primary way to create new
 * PrimitiveObjectInspector instances.
 * The reason of having caches here is that ObjectInspector is because
 * ObjectInspectors do not have an internal state - so ObjectInspectors with the
 * same construction parameters should result in exactly the same
 * ObjectInspector.
 */
public final class PrimitiveObjectInspectorFactory {

    public static final LazyBooleanObjectInspector LazyBooleanObjectInspector = new LazyBooleanObjectInspector();
    public static final LazyByteObjectInspector LazyByteObjectInspector = new LazyByteObjectInspector();
    public static final LazyShortObjectInspector LazyShortObjectInspector = new LazyShortObjectInspector();
    public static final LazyIntObjectInspector LazyIntObjectInspector = new LazyIntObjectInspector();
    public static final LazyLongObjectInspector LazyLongObjectInspector = new LazyLongObjectInspector();
    public static final LazyFloatObjectInspector LazyFloatObjectInspector = new LazyFloatObjectInspector();
    public static final LazyDoubleObjectInspector LazyDoubleObjectInspector = new LazyDoubleObjectInspector();
    public static final LazyStringObjectInspector LazyStringObjectInspector = new LazyStringObjectInspector(false,
            (byte) '\\');
    public static final LazyVoidObjectInspector LazyVoidObjectInspector = new LazyVoidObjectInspector();

    private static HashMap<PrimitiveCategory, AbstractPrimitiveLazyObjectInspector<?>> cachedPrimitiveLazyInspectorCache = new HashMap<PrimitiveCategory, AbstractPrimitiveLazyObjectInspector<?>>();

    static {
        cachedPrimitiveLazyInspectorCache.put(PrimitiveCategory.BOOLEAN, LazyBooleanObjectInspector);
        cachedPrimitiveLazyInspectorCache.put(PrimitiveCategory.BYTE, LazyByteObjectInspector);
        cachedPrimitiveLazyInspectorCache.put(PrimitiveCategory.SHORT, LazyShortObjectInspector);
        cachedPrimitiveLazyInspectorCache.put(PrimitiveCategory.INT, LazyIntObjectInspector);
        cachedPrimitiveLazyInspectorCache.put(PrimitiveCategory.LONG, LazyLongObjectInspector);
        cachedPrimitiveLazyInspectorCache.put(PrimitiveCategory.FLOAT, LazyFloatObjectInspector);
        cachedPrimitiveLazyInspectorCache.put(PrimitiveCategory.DOUBLE, LazyDoubleObjectInspector);
        cachedPrimitiveLazyInspectorCache.put(PrimitiveCategory.STRING, LazyStringObjectInspector);
        cachedPrimitiveLazyInspectorCache.put(PrimitiveCategory.VOID, LazyVoidObjectInspector);
    }

    /**
     * Returns the PrimitiveWritableObjectInspector for the PrimitiveCategory.
     * 
     * @param primitiveCategory
     */
    public static AbstractPrimitiveLazyObjectInspector<?> getPrimitiveLazyObjectInspector(
            PrimitiveCategory primitiveCategory) {
        AbstractPrimitiveLazyObjectInspector<?> result = cachedPrimitiveLazyInspectorCache.get(primitiveCategory);
        if (result == null) {
            throw new RuntimeException("Internal error: Cannot find ObjectInspector " + " for " + primitiveCategory);
        }
        return result;
    }

    private PrimitiveObjectInspectorFactory() {
        // prevent instantiation
    }
}
