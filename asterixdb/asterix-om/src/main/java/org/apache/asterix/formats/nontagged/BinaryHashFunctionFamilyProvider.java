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
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.data.IBinaryHashFunctionFamilyProvider;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;

/**
 * We use a binary hash function that promotes numeric types (tinyint,smallint,integer,bigint,float) to double.
 * Non-numeric types will be hashed without type promotion.
 */
public class BinaryHashFunctionFamilyProvider implements IBinaryHashFunctionFamilyProvider, Serializable {

    private static final long serialVersionUID = 1L;
    public static final BinaryHashFunctionFamilyProvider INSTANCE = new BinaryHashFunctionFamilyProvider();

    private BinaryHashFunctionFamilyProvider() {
    }

    @Override
    public IBinaryHashFunctionFamily getBinaryHashFunctionFamily(Object type) {
        // AMurmurHash3BinaryHashFunctionFamily converts numeric type to double type before doing hash()
        return new AMurmurHash3BinaryHashFunctionFamily((IAType) type);
    }
}
