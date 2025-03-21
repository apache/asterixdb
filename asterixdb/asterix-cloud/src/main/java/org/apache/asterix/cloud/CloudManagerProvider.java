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
package org.apache.asterix.cloud;

import org.apache.asterix.cloud.clients.ICloudGuardian;
import org.apache.asterix.common.api.INamespacePathResolver;
import org.apache.asterix.common.cloud.CloudCachePolicy;
import org.apache.asterix.common.cloud.IPartitionBootstrapper;
import org.apache.asterix.common.config.CloudProperties;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.control.nc.io.IOManager;

public class CloudManagerProvider {
    private CloudManagerProvider() {
    }

    public static IIOManager createIOManager(CloudProperties cloudProperties, IIOManager ioManager,
            INamespacePathResolver nsPathResolver, ICloudGuardian guardian) throws HyracksDataException {
        IOManager localIoManager = (IOManager) ioManager;
        if (cloudProperties.getCloudCachePolicy() == CloudCachePolicy.LAZY) {
            return new LazyCloudIOManager(localIoManager, cloudProperties, nsPathResolver, false, guardian);
        }

        return new EagerCloudIOManager(localIoManager, cloudProperties, nsPathResolver, guardian);
    }

    public static IPartitionBootstrapper getCloudPartitionBootstrapper(IIOManager ioManager) {
        if (!(ioManager instanceof AbstractCloudIOManager)) {
            throw new IllegalArgumentException("Not a cloud IOManager");
        }
        return (IPartitionBootstrapper) ioManager;
    }
}
