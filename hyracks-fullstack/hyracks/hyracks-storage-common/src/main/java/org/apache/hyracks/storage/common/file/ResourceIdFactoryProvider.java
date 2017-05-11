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
package org.apache.hyracks.storage.common.file;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.common.ILocalResourceRepository;

public class ResourceIdFactoryProvider {
    private ILocalResourceRepository localResourceRepository;

    public ResourceIdFactoryProvider(ILocalResourceRepository localResourceRepository) {
        this.localResourceRepository = localResourceRepository;
    }

    public ResourceIdFactory createResourceIdFactory() throws HyracksDataException {
        long largestResourceId = localResourceRepository.maxId();
        return new ResourceIdFactory(largestResourceId);
    }
}
