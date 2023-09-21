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
package org.apache.asterix.cloud.lazy.accessor;

import java.util.Collections;

import org.apache.asterix.cloud.clients.ICloudClient;
import org.apache.asterix.cloud.lazy.NoOpParallelCacher;
import org.apache.hyracks.control.nc.io.IOManager;

/**
 * Initial accessor to allow {@link org.apache.asterix.common.transactions.IGlobalTransactionContext} to work before
 * initializing the NC's partitions
 */
public class InitialCloudAccessor extends ReplaceableCloudAccessor {
    private static final ILazyAccessorReplacer NO_OP_REPLACER = () -> {
    };

    public InitialCloudAccessor(ICloudClient cloudClient, String bucket, IOManager localIoManager) {
        super(cloudClient, bucket, localIoManager, Collections.emptySet(), NO_OP_REPLACER, NoOpParallelCacher.INSTANCE);
    }
}
