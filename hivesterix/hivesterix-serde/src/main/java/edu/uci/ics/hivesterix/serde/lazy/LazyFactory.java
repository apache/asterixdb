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
package edu.uci.ics.hivesterix.serde.lazy;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

import edu.uci.ics.hivesterix.serde.lazy.objectinspector.LazyColumnarObjectInspector;
import edu.uci.ics.hivesterix.serde.lazy.objectinspector.LazyListObjectInspector;
import edu.uci.ics.hivesterix.serde.lazy.objectinspector.LazyMapObjectInspector;
import edu.uci.ics.hivesterix.serde.lazy.objectinspector.LazyStructObjectInspector;
import edu.uci.ics.hivesterix.serde.lazy.objectinspector.primitive.LazyBooleanObjectInspector;
import edu.uci.ics.hivesterix.serde.lazy.objectinspector.primitive.LazyByteObjectInspector;
import edu.uci.ics.hivesterix.serde.lazy.objectinspector.primitive.LazyDoubleObjectInspector;
import edu.uci.ics.hivesterix.serde.lazy.objectinspector.primitive.LazyFloatObjectInspector;
import edu.uci.ics.hivesterix.serde.lazy.objectinspector.primitive.LazyIntObjectInspector;
import edu.uci.ics.hivesterix.serde.lazy.objectinspector.primitive.LazyLongObjectInspector;
import edu.uci.ics.hivesterix.serde.lazy.objectinspector.primitive.LazyShortObjectInspector;
import edu.uci.ics.hivesterix.serde.lazy.objectinspector.primitive.LazyStringObjectInspector;

/**
 * LazyFactory.
 */
public final class LazyFactory {

    /**
     * Create a lazy binary primitive class given the type name.
     */
    public static LazyPrimitive<?, ?> createLazyPrimitiveClass(PrimitiveObjectInspector oi) {
        PrimitiveCategory p = oi.getPrimitiveCategory();
        switch (p) {
            case BOOLEAN:
                return new LazyBoolean((LazyBooleanObjectInspector) oi);
            case BYTE:
                return new LazyByte((LazyByteObjectInspector) oi);
            case SHORT:
                return new LazyShort((LazyShortObjectInspector) oi);
            case INT:
                return new LazyInteger((LazyIntObjectInspector) oi);
            case LONG:
                return new LazyLong((LazyLongObjectInspector) oi);
            case FLOAT:
                return new LazyFloat((LazyFloatObjectInspector) oi);
            case DOUBLE:
                return new LazyDouble((LazyDoubleObjectInspector) oi);
            case STRING:
                return new LazyString((LazyStringObjectInspector) oi);
            default:
                throw new RuntimeException("Internal error: no LazyObject for " + p);
        }
    }

    /**
     * Create a hierarchical LazyObject based on the given typeInfo.
     */
    public static LazyObject<? extends ObjectInspector> createLazyObject(ObjectInspector oi) {
        ObjectInspector.Category c = oi.getCategory();
        switch (c) {
            case PRIMITIVE:
                return createLazyPrimitiveClass((PrimitiveObjectInspector) oi);
            case MAP:
                return new LazyMap((LazyMapObjectInspector) oi);
            case LIST:
                return new LazyArray((LazyListObjectInspector) oi);
            case STRUCT: // check whether it is a top-level struct
                if (oi instanceof LazyStructObjectInspector)
                    return new LazyStruct((LazyStructObjectInspector) oi);
                else
                    return new LazyColumnar((LazyColumnarObjectInspector) oi);
            default:
                throw new RuntimeException("Hive LazySerDe Internal error.");
        }
    }

    private LazyFactory() {
        // prevent instantiation
    }
}
