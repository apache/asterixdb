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
package org.apache.hyracks.api.dataflow.value;

import java.io.Serializable;

/**
 * Ideally, {@code IBinaryHashFunctionFamily} should be stateless and thread-safe. Also, it should be made into
 * a singleton. However, this is implementation-dependent.
 * TODO: some existing implementations are not singleton and are stateful
 */
public interface IBinaryHashFunctionFamily extends Serializable {

    /**
     * Whether a singleton hash function instance is returned or a new hash function instance is created is
     * implementation-specific. Therefore, no assumption should be made in this regard.
     *
     * @param seed seed to be used by the hash function created
     *
     * @return a {@link IBinaryHashFunction} instance.
     */
    IBinaryHashFunction createBinaryHashFunction(int seed);
}
