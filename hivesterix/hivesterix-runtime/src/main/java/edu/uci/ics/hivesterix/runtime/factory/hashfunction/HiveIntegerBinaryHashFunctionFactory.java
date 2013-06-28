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
package edu.uci.ics.hivesterix.runtime.factory.hashfunction;

import edu.uci.ics.hivesterix.serde.lazy.LazyUtils;
import edu.uci.ics.hivesterix.serde.lazy.LazyUtils.VInt;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;

public class HiveIntegerBinaryHashFunctionFactory implements IBinaryHashFunctionFactory {
    private static final long serialVersionUID = 1L;

    public static IBinaryHashFunctionFactory INSTANCE = new HiveIntegerBinaryHashFunctionFactory();

    private HiveIntegerBinaryHashFunctionFactory() {
    }

    @Override
    public IBinaryHashFunction createBinaryHashFunction() {

        return new IBinaryHashFunction() {
            private VInt value = new VInt();

            @Override
            public int hash(byte[] bytes, int offset, int length) {
                LazyUtils.readVInt(bytes, offset, value);
                if (value.length != length)
                    throw new IllegalArgumentException("length mismatch in int hash function actual: " + length
                            + " expected " + value.length);
                return value.value;
            }
        };
    }

}
