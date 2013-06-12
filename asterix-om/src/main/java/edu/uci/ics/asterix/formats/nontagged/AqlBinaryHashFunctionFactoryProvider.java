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
package edu.uci.ics.asterix.formats.nontagged;

import java.io.Serializable;

import edu.uci.ics.hyracks.algebricks.data.IBinaryHashFunctionFactoryProvider;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.data.std.accessors.MurmurHash3BinaryHashFunctionFamily;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.RawUTF8StringPointable;

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
                return MurmurHash3BinaryHashFunctionFamily.INSTANCE.createBinaryHashFunction(0);
            }
        };
    }

}
