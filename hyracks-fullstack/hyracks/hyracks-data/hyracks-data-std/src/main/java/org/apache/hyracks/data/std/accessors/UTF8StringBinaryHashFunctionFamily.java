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
package org.apache.hyracks.data.std.accessors;

import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import org.apache.hyracks.util.string.UTF8StringUtil;

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
                return UTF8StringUtil.hash(bytes, offset, coefficient, r);
            }
        };
    }
}
