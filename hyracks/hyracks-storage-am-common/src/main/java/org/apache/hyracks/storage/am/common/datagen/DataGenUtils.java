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

package org.apache.hyracks.storage.am.common.datagen;

import java.util.Random;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;

@SuppressWarnings("rawtypes")
public class DataGenUtils {
    public static IFieldValueGenerator getFieldGenFromSerde(ISerializerDeserializer serde, Random rnd, boolean sorted) {
        if (serde instanceof IntegerSerializerDeserializer) {
            if (sorted) {
                return new SortedIntegerFieldValueGenerator();
            } else {
                return new IntegerFieldValueGenerator(rnd);
            }
        } else if (serde instanceof FloatSerializerDeserializer) {
            if (sorted) {
                return new SortedFloatFieldValueGenerator();
            } else {
                return new FloatFieldValueGenerator(rnd);
            }
        } else if (serde instanceof DoubleSerializerDeserializer) {
            if (sorted) {
                return new SortedDoubleFieldValueGenerator();
            } else {
                return new DoubleFieldValueGenerator(rnd);
            }
        } else if (serde instanceof UTF8StringSerializerDeserializer) {
            return new StringFieldValueGenerator(20, rnd);
        }
        return null;
    }

    public static IFieldValueGenerator[] getFieldGensFromSerdes(ISerializerDeserializer[] serdes, Random rnd,
            boolean sorted) {
        IFieldValueGenerator[] fieldValueGens = new IFieldValueGenerator[serdes.length];
        for (int i = 0; i < serdes.length; i++) {
            fieldValueGens[i] = getFieldGenFromSerde(serdes[i], rnd, sorted);
        }
        return fieldValueGens;
    }
}
