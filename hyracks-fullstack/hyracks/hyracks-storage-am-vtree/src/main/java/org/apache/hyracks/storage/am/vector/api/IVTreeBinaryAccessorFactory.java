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
package org.apache.hyracks.storage.am.vector.api;

import java.io.Serializable;

import org.apache.hyracks.api.io.IJsonSerializable;

/**
 * Serializable factory for {@link IVTreeBinaryAccessor}. Passed from the AsterixDB layer
 * to the Hyracks layer so storage code can construct accessors without a compile-time
 * dependency on AsterixDB modules.
 */
public interface IVTreeBinaryAccessorFactory extends Serializable, IJsonSerializable {

    /** Index-access-parameters key under which a factory instance is passed to the storage layer. */
    String IAP_KEY = "VECTOR_QUERY";

    IVTreeBinaryAccessor createAccessor();
}
