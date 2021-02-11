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
package org.apache.asterix.app.nc.task;

import static org.apache.asterix.api.http.server.NCUdfRecoveryServlet.GET_ALL_UDF_ENDPOINT;
import static org.apache.asterix.common.utils.Servlets.UDF_RECOVERY;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.asterix.common.api.INCLifecycleTask;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.http.client.utils.URIBuilder;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.service.IControllerService;
import org.apache.hyracks.util.file.FileUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RetrieveLibrariesTask implements INCLifecycleTask {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LogManager.getLogger();
    private final List<Pair<URI, String>> nodes;

    public RetrieveLibrariesTask(List<Pair<URI, String>> nodes) {
        this.nodes = nodes;
        if (nodes.size() <= 0) {
            throw new IllegalArgumentException("No nodes specified to retrieve from");
        }
    }

    @Override
    public void perform(CcId ccId, IControllerService cs) throws HyracksDataException {
        INcApplicationContext appContext = (INcApplicationContext) cs.getApplicationContext();
        boolean success = false;
        for (Pair<URI, String> referenceNode : nodes) {
            try {
                LOGGER.info("Retrieving UDFs from " + referenceNode.getFirst().getHost());
                retrieveLibrary(referenceNode.getFirst(), referenceNode.getSecond(), appContext);
                success = true;
                break;
            } catch (HyracksDataException e) {
                LOGGER.error("Unable to retrieve UDFs from: " + referenceNode.getFirst() + ", trying another node.", e);
            }
        }
        if (!success) {
            LOGGER.error("Unable to retrieve UDFs from any participant node");
            throw HyracksDataException.create(ErrorCode.TIMEOUT);
        }
    }

    private void retrieveLibrary(URI baseURI, String authToken, INcApplicationContext appContext)
            throws HyracksDataException {
        ILibraryManager libraryManager = appContext.getLibraryManager();
        FileReference distributionDir = appContext.getLibraryManager().getDistributionDir();
        URI libraryURI = getNCUdfRetrievalURL(baseURI);
        try {
            FileUtil.forceMkdirs(distributionDir.getFile());
            Path targetFile = Files.createTempFile(Paths.get(distributionDir.getAbsolutePath()), "all_", ".zip");
            FileReference targetFileRef = distributionDir.getChild(targetFile.getFileName().toString());
            libraryManager.download(targetFileRef, authToken, libraryURI);
            Path outputDirPath = libraryManager.getStorageDir().getFile().toPath().toAbsolutePath().normalize();
            FileReference outPath = appContext.getIoManager().resolveAbsolutePath(outputDirPath.toString());
            libraryManager.unzip(targetFileRef, outPath);
        } catch (IOException e) {
            LOGGER.error("Unable to retrieve UDFs from " + libraryURI.toString() + " before timeout");
            throw HyracksDataException.create(e);
        }
    }

    public URI getNCUdfRetrievalURL(URI baseURL) {
        String endpoint = UDF_RECOVERY.substring(0, UDF_RECOVERY.length() - 1) + GET_ALL_UDF_ENDPOINT;
        URIBuilder builder = new URIBuilder(baseURL).setPath(endpoint);
        try {
            return builder.build();
        } catch (URISyntaxException e) {
            LOGGER.error("Could not find URL for NC recovery", e);
        }
        return null;
    }

    @Override
    public String toString() {
        return "{ \"class\" : \"" + getClass().getSimpleName() + "\" }";
    }
}
