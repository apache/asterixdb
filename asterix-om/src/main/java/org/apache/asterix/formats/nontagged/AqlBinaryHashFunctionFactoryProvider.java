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
package org.apache.asterix.formats.nontagged;

import java.io.Serializable;

import org.apache.asterix.dataflow.data.nontagged.hash.AMurmurHash3BinaryHashFunctionFamily;
import org.apache.hyracks.algebricks.data.IBinaryHashFunctionFactoryProvider;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.RawUTF8StringPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringLowercasePointable;

public class AqlBinaryHashFunctionFactoryProvider implements IBinaryHashFunctionFactoryProvider, Serializable {

    private static final long serialVersionUID = 1L;
    public static final AqlBinaryHashFunctionFactoryProvider INSTANCE = new AqlBinaryHashFunctionFactoryProvider();
    public static final PointableBinaryHashFunctionFactory INTEGER_POINTABLE_INSTANCE = new PointableBinaryHashFunctionFactory(
            IntegerPointable.FACTORY);
    public static final PointableBinaryHashFunctionFactory FLOAT_POINTABLE_INSTANCE = new PointableBinaryHashFunctionFactory(
            FloatPointable.FACTORY);
    public static final PointableBinaryHashFunctionFactory DOUBLE_POINTABLE_INSTANCE = new PointableBinaryHashFunctionFactory(
            DoublePointable.FACTORY);
    public static final PointableBinaryHashFunctionFactory UTF8STRING_POINTABLE_INSTANCE = new PointableBinaryHashFunctionFactory(
            RawUTF8StringPointable.FACTORY);
    // Equivalent to UTF8STRING_POINTABLE_INSTANCE but all characters are considered lower case to implement case-insensitive hashing.
    public static final PointableBinaryHashFunctionFactory UTF8STRING_LOWERCASE_POINTABLE_INSTANCE = new PointableBinaryHashFunctionFactory(
            UTF8StringLowercasePointable.FACTORY);

    private AqlBinaryHashFunctionFactoryProvider() {
    }

    @Override
    public IBinaryHashFunctionFactory getBinaryHashFunctionFactory(Object type) {
        return new IBinaryHashFunctionFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public IBinaryHashFunction createBinaryHashFunction() {
                // Actual numeric type promotion happens in the createBinaryHashFunction()
                return AMurmurHash3BinaryHashFunctionFamily.INSTANCE.createBinaryHashFunction(0);
            }
        };
    }

}
