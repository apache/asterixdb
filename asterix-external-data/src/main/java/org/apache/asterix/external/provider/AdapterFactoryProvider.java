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
package org.apache.asterix.external.provider;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.adapter.factory.GenericAdapterFactory;
import org.apache.asterix.external.adapter.factory.LookupAdapterFactory;
import org.apache.asterix.external.api.IAdapterFactory;
import org.apache.asterix.external.api.IIndexingAdapterFactory;
import org.apache.asterix.external.dataset.adapter.GenericAdapter;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.external.library.ExternalLibraryManager;
import org.apache.asterix.external.runtime.GenericSocketFeedAdapter;
import org.apache.asterix.external.runtime.GenericSocketFeedAdapterFactory;
import org.apache.asterix.external.runtime.SocketClientAdapter;
import org.apache.asterix.external.runtime.SocketClientAdapterFactory;
import org.apache.asterix.external.util.ExternalDataCompatibilityUtils;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.dataflow.value.INullWriterFactory;

public class AdapterFactoryProvider {

    public static final Map<String, Class<? extends IAdapterFactory>> adapterFactories = initializeAdapterFactoryMapping();

    private static Map<String, Class<? extends IAdapterFactory>> initializeAdapterFactoryMapping() {
        Map<String, Class<? extends IAdapterFactory>> adapterFactories = new HashMap<String, Class<? extends IAdapterFactory>>();
        // Class names
        adapterFactories.put(GenericAdapter.class.getName(), GenericAdapterFactory.class);
        adapterFactories.put(GenericSocketFeedAdapter.class.getName(), GenericSocketFeedAdapterFactory.class);
        adapterFactories.put(SocketClientAdapter.class.getName(), SocketClientAdapterFactory.class);

        // Aliases
        adapterFactories.put(ExternalDataConstants.ALIAS_GENERIC_ADAPTER, GenericAdapterFactory.class);
        adapterFactories.put(ExternalDataConstants.ALIAS_HDFS_ADAPTER, GenericAdapterFactory.class);
        adapterFactories.put(ExternalDataConstants.ALIAS_LOCALFS_ADAPTER, GenericAdapterFactory.class);
        adapterFactories.put(ExternalDataConstants.ALIAS_SOCKET_ADAPTER, GenericSocketFeedAdapterFactory.class);
        adapterFactories.put(ExternalDataConstants.ALIAS_SOCKET_CLIENT_ADAPTER, SocketClientAdapterFactory.class);
        adapterFactories.put(ExternalDataConstants.ALIAS_FILE_FEED_ADAPTER, GenericAdapterFactory.class);

        // Compatability
        adapterFactories.put(ExternalDataConstants.ADAPTER_HDFS_CLASSNAME, GenericAdapterFactory.class);
        adapterFactories.put(ExternalDataConstants.ADAPTER_LOCALFS_CLASSNAME, GenericAdapterFactory.class);
        return adapterFactories;
    }

    public static IAdapterFactory getAdapterFactory(String adapterClassname, Map<String, String> configuration,
            ARecordType itemType) throws Exception {
        ExternalDataCompatibilityUtils.addCompatabilityParameters(adapterClassname, itemType, configuration);
        if (!adapterFactories.containsKey(adapterClassname)) {
            throw new AsterixException("Unknown adapter: " + adapterClassname);
        }
        IAdapterFactory adapterFactory = adapterFactories.get(adapterClassname).newInstance();
        adapterFactory.configure(configuration, itemType);
        return adapterFactory;
    }

    public static IIndexingAdapterFactory getAdapterFactory(String adapterClassname, Map<String, String> configuration,
            ARecordType itemType, List<ExternalFile> snapshot, boolean indexingOp)
                    throws AsterixException, InstantiationException, IllegalAccessException {
        ExternalDataCompatibilityUtils.addCompatabilityParameters(adapterClassname, itemType, configuration);
        if (!adapterFactories.containsKey(adapterClassname)) {
            throw new AsterixException("Unknown adapter");
        }
        try {
            IIndexingAdapterFactory adapterFactory = (IIndexingAdapterFactory) adapterFactories.get(adapterClassname)
                    .newInstance();
            adapterFactory.setSnapshot(snapshot, indexingOp);
            adapterFactory.configure(configuration, itemType);
            return adapterFactory;
        } catch (Exception e) {
            throw new AsterixException("Failed to create indexing adapter factory.", e);
        }
    }

    @SuppressWarnings("unchecked")
    public static void addNewAdapter(String dataverseName, String adapterClassName, String adapterAlias,
            String adapterFactoryClassName, String libraryName) throws ClassNotFoundException {
        ClassLoader classLoader = ExternalLibraryManager.getLibraryClassLoader(dataverseName, libraryName);
        Class<? extends IAdapterFactory> adapterFactoryClass = (Class<? extends IAdapterFactory>) classLoader
                .loadClass(adapterFactoryClassName);
        adapterFactories.put(adapterClassName, adapterFactoryClass);
        adapterFactories.put(adapterAlias, adapterFactoryClass);
    }

    public static LookupAdapterFactory<?> getAdapterFactory(Map<String, String> configuration, ARecordType recordType,
            int[] ridFields, boolean retainInput, boolean retainNull, INullWriterFactory iNullWriterFactory)
                    throws Exception {
        LookupAdapterFactory<?> adapterFactory = new LookupAdapterFactory<>(recordType, ridFields, retainInput,
                retainNull, iNullWriterFactory);
        adapterFactory.configure(configuration);
        return adapterFactory;
    }
}
