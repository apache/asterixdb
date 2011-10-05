/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.hyracks.dataflow.common.util;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.DoubleBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.FloatBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.IntegerBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.UTF8StringBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.BooleanSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;

@SuppressWarnings("rawtypes")
public class SerdeUtils {
    public static ITypeTrait[] serdesToTypeTraits(ISerializerDeserializer[] serdes, int numSerdes) {
        ITypeTrait[] typeTraits = new ITypeTrait[numSerdes];
        for (int i = 0; i < numSerdes; i++) {
            typeTraits[i] = serdeToTypeTrait(serdes[i]);
        }
        return typeTraits;
    }

    public static ITypeTrait serdeToTypeTrait(ISerializerDeserializer serde) {
        if (serde instanceof IntegerSerializerDeserializer) {
            return ITypeTrait.INTEGER_TYPE_TRAIT;
        }
        if (serde instanceof Integer64SerializerDeserializer) {
            return ITypeTrait.INTEGER64_TYPE_TRAIT;
        }
        if (serde instanceof FloatSerializerDeserializer) {
            return ITypeTrait.FLOAT_TYPE_TRAIT;
        }
        if (serde instanceof DoubleSerializerDeserializer) {
            return ITypeTrait.DOUBLE_TYPE_TRAIT;
        }
        if (serde instanceof BooleanSerializerDeserializer) {
            return ITypeTrait.BOOLEAN_TYPE_TRAIT;
        }
        return ITypeTrait.VARLEN_TYPE_TRAIT;
    }

    public static IBinaryComparator[] serdesToComparators(ISerializerDeserializer[] serdes, int numSerdes) {
        IBinaryComparator[] comparators = new IBinaryComparator[numSerdes];
        for (int i = 0; i < numSerdes; i++) {
            comparators[i] = serdeToComparator(serdes[i]);
        }
        return comparators;
    }

    public static IBinaryComparator serdeToComparator(ISerializerDeserializer serde) {
        if (serde instanceof IntegerSerializerDeserializer) {
            return IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        }
        if (serde instanceof Integer64SerializerDeserializer) {
            throw new UnsupportedOperationException("Binary comparator for Integer64 not implemented.");
        }
        if (serde instanceof FloatSerializerDeserializer) {
            return FloatBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        }
        if (serde instanceof DoubleSerializerDeserializer) {
            return DoubleBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        }
        if (serde instanceof BooleanSerializerDeserializer) {
            throw new UnsupportedOperationException("Binary comparator for Boolean not implemented.");
        }
        if (serde instanceof UTF8StringSerializerDeserializer) {
            return UTF8StringBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        }
        throw new UnsupportedOperationException("Binary comparator for + " + serde.toString() + " not implemented.");
    }
}
