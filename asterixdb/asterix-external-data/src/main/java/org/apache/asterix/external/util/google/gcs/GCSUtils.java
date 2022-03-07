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
package org.apache.asterix.external.util.google.gcs;

import static org.apache.asterix.common.exceptions.ErrorCode.EXTERNAL_SOURCE_ERROR;
import static org.apache.asterix.common.exceptions.ErrorCode.INVALID_REQ_PARAM_VAL;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_FORMAT;
import static org.apache.asterix.external.util.ExternalDataUtils.getPrefix;
import static org.apache.asterix.external.util.ExternalDataUtils.isParquetFormat;
import static org.apache.asterix.external.util.ExternalDataUtils.validateIncludeExclude;
import static org.apache.asterix.external.util.google.gcs.GCSConstants.JSON_CREDENTIALS_FIELD_NAME;
import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class GCSUtils {
    private GCSUtils() {
        throw new AssertionError("do not instantiate");

    }

    //TODO(htowaileb): Add validation step similar to other externals, which also checks if empty bucket
    //upon creating the external dataset

    /**
     * Builds the client using the provided configuration
     *
     * @param configuration properties
     * @return clientasterixdb/asterix-external-data/src/main/java/org/apache/asterix/external/util/ExternalDataUtils.java
     * @throws CompilationException CompilationException
     */
    public static Storage buildClient(Map<String, String> configuration) throws CompilationException {
        String jsonCredentials = configuration.get(JSON_CREDENTIALS_FIELD_NAME);

        StorageOptions.Builder builder = StorageOptions.newBuilder();

        // Use credentials if available
        if (jsonCredentials != null) {
            try (InputStream credentialsStream = new ByteArrayInputStream(jsonCredentials.getBytes())) {
                builder.setCredentials(ServiceAccountCredentials.fromStream(credentialsStream));
            } catch (IOException ex) {
                throw new CompilationException(EXTERNAL_SOURCE_ERROR, getMessageOrToString(ex));
            }
        }

        return builder.build().getService();
    }

    /**
     * Validate external dataset properties
     *
     * @param configuration properties
     * @throws CompilationException Compilation exception
     */
    public static void validateProperties(Map<String, String> configuration, SourceLocation srcLoc,
            IWarningCollector collector) throws CompilationException {

        // check if the format property is present
        if (configuration.get(ExternalDataConstants.KEY_FORMAT) == null) {
            throw new CompilationException(ErrorCode.PARAMETERS_REQUIRED, srcLoc, ExternalDataConstants.KEY_FORMAT);
        }

        // parquet is not supported for google cloud storage
        if (isParquetFormat(configuration)) {
            throw new CompilationException(INVALID_REQ_PARAM_VAL, srcLoc, KEY_FORMAT, configuration.get(KEY_FORMAT));
        }

        validateIncludeExclude(configuration);
        String container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);

        try {
            Storage.BlobListOption limitOption = Storage.BlobListOption.pageSize(1);
            Storage.BlobListOption prefixOption = Storage.BlobListOption.prefix(getPrefix(configuration));
            Storage storage = buildClient(configuration);
            Page<Blob> items = storage.list(container, limitOption, prefixOption);

            if (!items.iterateAll().iterator().hasNext() && collector.shouldWarn()) {
                Warning warning = Warning.of(srcLoc, ErrorCode.EXTERNAL_SOURCE_CONFIGURATION_RETURNED_NO_FILES);
                collector.warn(warning);
            }
        } catch (CompilationException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, getMessageOrToString(ex));
        }
    }
}
