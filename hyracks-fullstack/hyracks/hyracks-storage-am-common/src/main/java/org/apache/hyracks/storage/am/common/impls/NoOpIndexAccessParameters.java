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
package org.apache.hyracks.storage.am.common.impls;

import java.util.Collections;
import java.util.Map;

import org.apache.hyracks.storage.am.common.api.IExtendedModificationOperationCallback;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.ISearchOperationCallback;

public class NoOpIndexAccessParameters implements IIndexAccessParameters {
    public static final NoOpIndexAccessParameters INSTANCE = new NoOpIndexAccessParameters();
    // Immutable empty map not to cause getParameters().get() generate an exception
    private static final Map<String, Object> paramMap = Collections.emptyMap();

    private NoOpIndexAccessParameters() {
    }

    @Override
    public IExtendedModificationOperationCallback getModificationCallback() {
        return NoOpOperationCallback.INSTANCE;
    }

    @Override
    public ISearchOperationCallback getSearchOperationCallback() {
        return NoOpOperationCallback.INSTANCE;
    }

    @Override
    public Map<String, Object> getParameters() {
        return paramMap;
    }
}
