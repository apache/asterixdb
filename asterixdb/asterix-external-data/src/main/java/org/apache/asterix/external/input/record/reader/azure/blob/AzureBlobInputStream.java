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
package org.apache.asterix.external.input.record.reader.azure.blob;

import static org.apache.asterix.external.util.azure.blob_storage.AzureUtils.buildAzureBlobClient;
import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.input.filter.embedder.IExternalFilterValueEmbedder;
import org.apache.asterix.external.input.record.reader.abstracts.AbstractExternalInputStream;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.LogRedactionUtil;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;

public class AzureBlobInputStream extends AbstractExternalInputStream {

    private final BlobServiceClient client;
    private final String container;

    public AzureBlobInputStream(IApplicationContext appCtx, Map<String, String> configuration, List<String> filePaths,
            IExternalFilterValueEmbedder valueEmbedder) throws HyracksDataException {
        super(configuration, filePaths, valueEmbedder);
        this.client = buildAzureClient(appCtx, configuration);
        this.container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
    }

    @Override
    protected boolean getInputStream() throws IOException {
        String fileName = filePaths.get(nextFileIndex);
        BlobContainerClient blobContainerClient;
        BlobClient blob;
        try {
            blobContainerClient = client.getBlobContainerClient(container);
            blob = blobContainerClient.getBlobClient(filePaths.get(nextFileIndex));
            in = blob.openInputStream();

            // Use gzip stream if needed
            String lowerCaseFileName = fileName.toLowerCase();
            if (lowerCaseFileName.endsWith(".gz") || lowerCaseFileName.endsWith(".gzip")) {
                in = new GZIPInputStream(in, ExternalDataConstants.DEFAULT_BUFFER_SIZE);
            }
        } catch (BlobStorageException ex) {
            if (ex.getErrorCode().equals(BlobErrorCode.BLOB_NOT_FOUND)) {
                LOGGER.debug(() -> "Key " + LogRedactionUtil.userData(filePaths.get(nextFileIndex)) + " was not "
                        + "found in container " + container);
                return false;
            } else {
                throw new RuntimeDataException(ErrorCode.EXTERNAL_SOURCE_ERROR, getMessageOrToString(ex));
            }
        } catch (Exception ex) {
            throw new RuntimeDataException(ErrorCode.EXTERNAL_SOURCE_ERROR, getMessageOrToString(ex));
        }

        return true;
    }

    private BlobServiceClient buildAzureClient(IApplicationContext appCtx, Map<String, String> configuration)
            throws HyracksDataException {
        try {
            return buildAzureBlobClient(appCtx, configuration);
        } catch (CompilationException ex) {
            throw HyracksDataException.create(ex);
        }
    }
}
