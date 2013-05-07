/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.common.api;

import java.util.logging.Logger;

import edu.uci.ics.asterix.common.config.AsterixCompilerProperties;
import edu.uci.ics.asterix.common.config.AsterixExternalProperties;
import edu.uci.ics.asterix.common.config.AsterixMetadataProperties;
import edu.uci.ics.asterix.common.config.AsterixPropertiesAccessor;
import edu.uci.ics.asterix.common.config.AsterixStorageProperties;
import edu.uci.ics.asterix.common.config.AsterixTransactionProperties;
import edu.uci.ics.asterix.common.config.IAsterixPropertiesProvider;
import edu.uci.ics.asterix.common.context.AsterixRuntimeComponentsProvider;
import edu.uci.ics.asterix.common.dataflow.IAsterixApplicationContextInfo;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.hyracks.api.application.ICCApplicationContext;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

/*
 * Acts as an holder class for IndexRegistryProvider, AsterixStorageManager
 * instances that are accessed from the NCs. In addition an instance of ICCApplicationContext 
 * is stored for access by the CC.
 */
public class AsterixAppContextInfoImpl implements IAsterixApplicationContextInfo, IAsterixPropertiesProvider {

    private static AsterixAppContextInfoImpl INSTANCE;

    private final ICCApplicationContext appCtx;

    private static AsterixCompilerProperties compilerProperties;
    private static AsterixExternalProperties externalProperties;
    private static AsterixMetadataProperties metadataProperties;
    private static AsterixStorageProperties storageProperties;
    private static AsterixTransactionProperties txnProperties;

    public static void initialize(ICCApplicationContext ccAppCtx) throws AsterixException {
        if (INSTANCE == null) {
            INSTANCE = new AsterixAppContextInfoImpl(ccAppCtx);
        }
        AsterixPropertiesAccessor propertiesAccessor = new AsterixPropertiesAccessor();
        compilerProperties = new AsterixCompilerProperties(propertiesAccessor);
        externalProperties = new AsterixExternalProperties(propertiesAccessor);
        metadataProperties = new AsterixMetadataProperties(propertiesAccessor);
        storageProperties = new AsterixStorageProperties(propertiesAccessor);
        txnProperties = new AsterixTransactionProperties(propertiesAccessor);

        Logger.getLogger(".").setLevel(externalProperties.getLogLevel());
    }

    private AsterixAppContextInfoImpl(ICCApplicationContext ccAppCtx) {
        this.appCtx = ccAppCtx;
    }

    public static IAsterixApplicationContextInfo getInstance() {
        return INSTANCE;
    }

    @Override
    public IStorageManagerInterface getStorageManagerInterface() {
        return AsterixRuntimeComponentsProvider.NOINDEX_PROVIDER;
    }

    @Override
    public ICCApplicationContext getCCApplicationContext() {
        return appCtx;
    }

    @Override
    public IIndexLifecycleManagerProvider getIndexLifecycleManagerProvider() {
        return AsterixRuntimeComponentsProvider.NOINDEX_PROVIDER;
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
}
