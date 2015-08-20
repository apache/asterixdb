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
package edu.uci.ics.hyracks.data.std.accessors;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public class UTF8StringBinaryHashFunctionFamily implements IBinaryHashFunctionFamily {
    public static final IBinaryHashFunctionFamily INSTANCE = new UTF8StringBinaryHashFunctionFamily();

    private static final long serialVersionUID = 1L;

    static final int[] primeCoefficents = { 31, 23, 53, 97, 71, 337, 11, 877, 3, 29 };

    private UTF8StringBinaryHashFunctionFamily() {
    }

    @Override
    public IBinaryHashFunction createBinaryHashFunction(int seed) {
        final int coefficient = primeCoefficents[seed % primeCoefficents.length];
        final int r = primeCoefficents[(seed + 1) % primeCoefficents.length];

        return new IBinaryHashFunction() {
            @Override
            public int hash(byte[] bytes, int offset, int length) {
                int h = 0;
                int utflen = UTF8StringPointable.getUTFLength(bytes, offset);
                int sStart = offset + 2;
                int c = 0;

                while (c < utflen) {
                    char ch = UTF8StringPointable.charAt(bytes, sStart + c);
                    h = (coefficient * h + ch) % r;
                    c += UTF8StringPointable.charSize(bytes, sStart + c);
                }
                return h;
            }
        };
    }
}