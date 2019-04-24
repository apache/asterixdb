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

package org.apache.hyracks.dataflow.common.utils;

import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.data.std.accessors.BooleanBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.DoubleBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.FloatBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.LongBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.ShortBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FixedLengthTypeTrait;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.BooleanSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.ShortSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;

@SuppressWarnings("rawtypes")
public class SerdeUtils {

    public static ITypeTraits[] serdesToTypeTraits(ISerializerDeserializer[] serdes) {
        ITypeTraits[] typeTraits = new ITypeTraits[serdes.length];
        for (int i = 0; i < serdes.length; i++) {
            typeTraits[i] = serdeToTypeTrait(serdes[i]);
        }
        return typeTraits;
    }

    public static ITypeTraits[] serdesToTypeTraits(ISerializerDeserializer[] serdes, int payloadSize) {
        ITypeTraits[] typeTraits = new ITypeTraits[serdes.length + 1];
        for (int i = 0; i < serdes.length; i++) {
            typeTraits[i] = serdeToTypeTrait(serdes[i]);
        }
        typeTraits[serdes.length] = new FixedLengthTypeTrait(payloadSize);
        return typeTraits;
    }

    public static ITypeTraits serdeToTypeTrait(ISerializerDeserializer serde) {
        if (serde instanceof ShortSerializerDeserializer) {
            return ShortPointable.TYPE_TRAITS;
        }
        if (serde instanceof IntegerSerializerDeserializer) {
            return IntegerPointable.TYPE_TRAITS;
        }
        if (serde instanceof Integer64SerializerDeserializer) {
            return LongPointable.TYPE_TRAITS;
        }
        if (serde instanceof FloatSerializerDeserializer) {
            return FloatPointable.TYPE_TRAITS;
        }
        if (serde instanceof DoubleSerializerDeserializer) {
            return DoublePointable.TYPE_TRAITS;
        }
        if (serde instanceof BooleanSerializerDeserializer) {
            return BooleanPointable.TYPE_TRAITS;
        }
        return UTF8StringPointable.TYPE_TRAITS;
    }

    public static IBinaryComparator[] serdesToComparators(ISerializerDeserializer[] serdes, int numSerdes) {
        IBinaryComparator[] comparators = new IBinaryComparator[numSerdes];
        for (int i = 0; i < numSerdes; i++) {
            comparators[i] = serdeToComparator(serdes[i]);
        }
        return comparators;
    }

    public static IBinaryComparator serdeToComparator(ISerializerDeserializer serde) {
        IBinaryComparatorFactory f = serdeToComparatorFactory(serde);
        return f.createBinaryComparator();
    }

    public static IBinaryComparatorFactory[] serdesToComparatorFactories(ISerializerDeserializer[] serdes,
            int numSerdes) {
        IBinaryComparatorFactory[] comparatorsFactories = new IBinaryComparatorFactory[numSerdes];
        for (int i = 0; i < numSerdes; i++) {
            comparatorsFactories[i] = serdeToComparatorFactory(serdes[i]);
        }
        return comparatorsFactories;
    }

    public static IBinaryComparatorFactory serdeToComparatorFactory(ISerializerDeserializer serde) {
        if (serde instanceof ShortSerializerDeserializer) {
            return ShortBinaryComparatorFactory.INSTANCE;
        }
        if (serde instanceof IntegerSerializerDeserializer) {
            return IntegerBinaryComparatorFactory.INSTANCE;
        }
        if (serde instanceof Integer64SerializerDeserializer) {
            return LongBinaryComparatorFactory.INSTANCE;
        }
        if (serde instanceof FloatSerializerDeserializer) {
            return FloatBinaryComparatorFactory.INSTANCE;
        }
        if (serde instanceof DoubleSerializerDeserializer) {
            return DoubleBinaryComparatorFactory.INSTANCE;
        }
        if (serde instanceof BooleanSerializerDeserializer) {
            return BooleanBinaryComparatorFactory.INSTANCE;
        }
        if (serde instanceof UTF8StringSerializerDeserializer) {
            return UTF8StringBinaryComparatorFactory.INSTANCE;
        }
        throw new UnsupportedOperationException("Binary comparator for + " + serde.toString() + " not implemented.");
    }
}
