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

package org.apache.asterix.common.metadata;

import java.io.File;

import org.apache.asterix.common.api.INamespacePathResolver;
import org.apache.asterix.common.utils.StoragePathUtil;

public class NamespacePathResolver implements INamespacePathResolver {

    private final boolean usingDatabase;

    public NamespacePathResolver(boolean usingDatabase) {
        this.usingDatabase = usingDatabase;
    }

    @Override
    public String resolve(Namespace namespace) {
        DataverseName dataverseName = namespace.getDataverseName();
        if (usingDatabase) {
            if (MetadataConstants.METADATA_DATAVERSE_NAME.equals(dataverseName)) {
                return StoragePathUtil.prepareDataverseName(dataverseName);
            }
            return namespace.getDatabaseName() + File.separatorChar
                    + StoragePathUtil.prepareDataverseName(dataverseName);
        } else {
            return StoragePathUtil.prepareDataverseName(dataverseName);
        }
    }

    @Override
    public String resolve(String databaseName, DataverseName dataverseName) {
        if (usingDatabase) {
            if (MetadataConstants.METADATA_DATAVERSE_NAME.equals(dataverseName)) {
                return StoragePathUtil.prepareDataverseName(dataverseName);
            }
            return databaseName + File.separatorChar + StoragePathUtil.prepareDataverseName(dataverseName);
        } else {
            return StoragePathUtil.prepareDataverseName(dataverseName);
        }
    }

    @Override
    public boolean usingDatabase() {
        return usingDatabase;
    }
}
