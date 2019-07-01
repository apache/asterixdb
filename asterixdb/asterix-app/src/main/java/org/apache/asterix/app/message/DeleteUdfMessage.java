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
package org.apache.asterix.app.message;

import org.apache.asterix.app.external.ExternalLibraryUtils;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DeleteUdfMessage implements INcAddressedMessage {

    private static final long serialVersionUID = -3129473321451281271L;
    private final String dataverseName;
    private final String libraryName;
    private static final Logger LOGGER = LogManager.getLogger();

    public DeleteUdfMessage(String dataverseName, String libraryName) {
        this.dataverseName = dataverseName;
        this.libraryName = libraryName;
    }

    @Override
    public void handle(INcApplicationContext appCtx) {
        ILibraryManager mgr = appCtx.getLibraryManager();
        String mdNodeName = appCtx.getMetadataProperties().getMetadataNodeName();
        String nodeName = appCtx.getServiceContext().getNodeId();
        boolean isMdNode = mdNodeName.equals(nodeName);
        try {
            if (isMdNode) {
                ExternalLibraryUtils.uninstallLibrary(dataverseName, libraryName);
            }
            mgr.deregisterLibraryClassLoader(dataverseName, libraryName);
        } catch (Exception e) {
            LOGGER.error("Unable to un-deploy UDF", e);
        }
    }
}
