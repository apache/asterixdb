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
package edu.uci.ics.asterix.dataflow.data.nontagged.hash;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;

public class DoubleBinaryHashFunctionFactory implements IBinaryHashFunctionFactory {

    private static final long serialVersionUID = 1L;

    public static final DoubleBinaryHashFunctionFactory INSTANCE = new DoubleBinaryHashFunctionFactory();

    private DoubleBinaryHashFunctionFactory() {
    }

    @Override
    public IBinaryHashFunction createBinaryHashFunction() {
        return new IBinaryHashFunction() {
            @Override
            public int hash(byte[] bytes, int offset, int length) {
                // copied from Double.hashCode()
                long bits = ADoubleSerializerDeserializer.getLongBits(bytes, offset);
                return (int) (bits ^ (bits >>> 32));
            }
        };
    }

}
