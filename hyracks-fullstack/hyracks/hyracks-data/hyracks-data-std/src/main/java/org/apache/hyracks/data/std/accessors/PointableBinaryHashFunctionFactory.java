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
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.data.std.api.IHashable;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IPointableFactory;

public class PointableBinaryHashFunctionFactory implements IBinaryHashFunctionFactory {
    private static final long serialVersionUID = 1L;

    private final IPointableFactory pf;

    public static PointableBinaryHashFunctionFactory of(IPointableFactory pf) {
        return new PointableBinaryHashFunctionFactory(pf);
    }

    public PointableBinaryHashFunctionFactory(IPointableFactory pf) {
        this.pf = pf;
    }

    @Override
    public IBinaryHashFunction createBinaryHashFunction() {
        final IPointable p = pf.createPointable();
        return new IBinaryHashFunction() {
            @Override
            public int hash(byte[] bytes, int offset, int length) {
                p.set(bytes, offset, length);
                return ((IHashable) p).hash();
            }
        };
    }
}
