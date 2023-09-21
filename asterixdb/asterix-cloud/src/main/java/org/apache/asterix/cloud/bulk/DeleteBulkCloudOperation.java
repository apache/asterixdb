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
package org.apache.asterix.cloud.bulk;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.asterix.cloud.clients.ICloudClient;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.control.nc.io.bulk.DeleteBulkOperation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DeleteBulkCloudOperation extends DeleteBulkOperation {
    private static final Logger LOGGER = LogManager.getLogger();
    private final String bucket;
    private final ICloudClient cloudClient;
    private final IBulkOperationCallBack callBack;

    public DeleteBulkCloudOperation(IOManager ioManager, String bucket, ICloudClient cloudClient,
            IBulkOperationCallBack callBack) {
        super(ioManager);
        this.bucket = bucket;
        this.cloudClient = cloudClient;
        this.callBack = callBack;
    }

    @Override
    public int performOperation() throws HyracksDataException {
        /*
         * TODO What about deleting multiple directories?
         *      Actually, is there a case where we delete multiple directories from the cloud?
         */
        List<String> paths = fileReferences.stream().map(FileReference::getRelativePath).collect(Collectors.toList());
        if (paths.isEmpty()) {
            return 0;
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Bulk deleting: local: {}, cloud: {}", fileReferences, paths);
        }
        cloudClient.deleteObjects(bucket, paths);
        // Bulk delete locally as well
        super.performOperation();
        callBack.call(fileReferences);
        return fileReferences.size();
    }
}
