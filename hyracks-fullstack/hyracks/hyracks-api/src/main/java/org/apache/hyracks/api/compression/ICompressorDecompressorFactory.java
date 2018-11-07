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
package org.apache.hyracks.api.compression;

import java.io.Serializable;

import org.apache.hyracks.api.io.IJsonSerializable;

/**
 * {@link ICompressorDecompressor} factory.
 *
 * New factory of this interface must implement two methods as well if the compression is intended for storage:
 * - {@link IJsonSerializable#toJson(org.apache.hyracks.api.io.IPersistedResourceRegistry)}
 * - a static method fromJson(IPersistedResourceRegistry registry, JsonNode json)
 */
public interface ICompressorDecompressorFactory extends Serializable, IJsonSerializable {
    /**
     * Create a compressor/decompressor instance
     *
     * @return {@code ICompressorDecompressor}
     */
    ICompressorDecompressor createInstance();
}
