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
package edu.uci.ics.hyracks.hdfs.lib;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;

public class RawBinaryHashFunctionFactory implements IBinaryHashFunctionFactory {
    private static final long serialVersionUID = 1L;

    public static IBinaryHashFunctionFactory INSTANCE = new RawBinaryHashFunctionFactory();

    private RawBinaryHashFunctionFactory() {
    }

    @Override
    public IBinaryHashFunction createBinaryHashFunction() {

        return new IBinaryHashFunction() {
            @Override
            public int hash(byte[] bytes, int offset, int length) {
                int value = 1;
                int end = offset + length;
                for (int i = offset; i < end; i++)
                    value = value * 31 + (int) bytes[i];
                return value;
            }
        };
    }

}