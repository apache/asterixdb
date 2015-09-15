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
package org.apache.asterix.om.util;

import java.util.logging.Logger;

import org.apache.asterix.common.config.AsterixCompilerProperties;
import org.apache.asterix.common.config.AsterixExternalProperties;
import org.apache.asterix.common.config.AsterixFeedProperties;
import org.apache.asterix.common.config.AsterixMetadataProperties;
import org.apache.asterix.common.config.AsterixPropertiesAccessor;
import org.apache.asterix.common.config.AsterixStorageProperties;
import org.apache.asterix.common.config.AsterixTransactionProperties;
import org.apache.asterix.common.config.IAsterixPropertiesProvider;
import org.apache.asterix.common.dataflow.IAsterixApplicationContextInfo;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.transaction.management.service.transaction.AsterixRuntimeComponentsProvider;
import org.apache.hyracks.api.application.ICCApplicationContext;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import org.apache.hyracks.storage.common.IStorageManagerInterface;

/*
 * Acts as an holder class for IndexRegistryProvider, AsterixStorageManager
 * instances that are accessed from the NCs. In addition an instance of ICCApplicationContext 
 * is stored for access by the CC.
 */
public class AsterixAppContextInfo implements IAsterixApplicationContextInfo, IAsterixPropertiesProvider {

    private static AsterixAppContextInfo INSTANCE;
    private final ICCApplicationContext appCtx;

    private AsterixCompilerProperties compilerProperties;
    private AsterixExternalProperties externalProperties;
    private AsterixMetadataProperties metadataProperties;
    private AsterixStorageProperties storageProperties;
    private AsterixTransactionProperties txnProperties;
    private AsterixFeedProperties feedProperties;

    private IHyracksClientConnection hcc;

    public static void initialize(ICCApplicationContext ccAppCtx, IHyracksClientConnection hcc) throws AsterixException {
        if (INSTANCE == null) {
            INSTANCE = new AsterixAppContextInfo(ccAppCtx, hcc);
        }
        AsterixPropertiesAccessor propertiesAccessor = new AsterixPropertiesAccessor();
        INSTANCE.compilerProperties = new AsterixCompilerProperties(propertiesAccessor);
        INSTANCE.externalProperties = new AsterixExternalProperties(propertiesAccessor);
        INSTANCE.metadataProperties = new AsterixMetadataProperties(propertiesAccessor);
        INSTANCE.storageProperties = new AsterixStorageProperties(propertiesAccessor);
        INSTANCE.txnProperties = new AsterixTransactionProperties(propertiesAccessor);
        INSTANCE.feedProperties = new AsterixFeedProperties(propertiesAccessor);
        INSTANCE.hcc = hcc;
        Logger.getLogger("org.apache").setLevel(INSTANCE.externalProperties.getLogLevel());
    }

    private AsterixAppContextInfo(ICCApplicationContext ccAppCtx, IHyracksClientConnection hcc) {
        this.appCtx = ccAppCtx;
        this.hcc = hcc;
    }

    public static AsterixAppContextInfo getInstance() {
        return INSTANCE;
    }

    @Override
    public ICCApplicationContext getCCApplicationContext() {
        return appCtx;
    }

    @Override
    public AsterixStorageProperties getStorageProperties() {
        return storageProperties;
    }

    @Override
    public AsterixTransactionProperties getTransactionProperties() {
        return txnProperties;
    }

    @Override
    public AsterixCompilerProperties getCompilerProperties() {
        return compilerProperties;
    }

    @Override
    public AsterixMetadataProperties getMetadataProperties() {
        return metadataProperties;
    }

    @Override
    public AsterixExternalProperties getExternalProperties() {
        return externalProperties;
    }

    @Override
    public AsterixFeedProperties getFeedProperties() {
        return feedProperties;
    }
    
    public IHyracksClientConnection getHcc() {
        return hcc;
    }

    @Override
    public IIndexLifecycleManagerProvider getIndexLifecycleManagerProvider() {
        return AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER;
    }

    @Override
    public IStorageManagerInterface getStorageManagerInterface() {
        return AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER;
    }
}
