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
package org.apache.asterix.app.nc;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.algebra.base.ILangExtension;
import org.apache.asterix.common.api.ExtensionId;
import org.apache.asterix.common.api.IExtension;
import org.apache.asterix.common.api.INamespaceResolver;
import org.apache.asterix.common.config.AsterixExtension;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.compiler.provider.SqlppCompilationProvider;
import org.apache.asterix.metadata.api.IMetadataExtension;
import org.apache.asterix.metadata.api.INCExtensionManager;
import org.apache.asterix.metadata.bootstrap.MetadataIndexesProvider;
import org.apache.asterix.metadata.entitytupletranslators.MetadataTupleTranslatorProvider;
import org.apache.asterix.utils.ExtensionUtil;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * AsterixDB's implementation of {@code INCExtensionManager} which takes care of
 * initializing extensions on Node Controllers
 */
public class NCExtensionManager implements INCExtensionManager {

    private final ILangCompilationProvider sqlppCompilationProvider;
    private final MetadataTupleTranslatorProvider tupleTranslatorProvider;
    private final MetadataIndexesProvider metadataIndexesProvider;
    private final List<IMetadataExtension> mdExtensions;

    /**
     * Initialize {@code CCExtensionManager} from configuration
     *
     * @param list
     *         list of user configured extensions
     * @throws InstantiationException
     *         if an extension couldn't be created
     * @throws IllegalAccessException
     *         if user doesn't have enough acess priveleges
     * @throws ClassNotFoundException
     *         if a class was not found
     * @throws HyracksDataException
     *         if two extensions conlict with each other
     */
    public NCExtensionManager(List<AsterixExtension> list, boolean usingDatabase, INamespaceResolver namespaceResolver)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException, HyracksDataException {
        Pair<ExtensionId, ILangCompilationProvider> sqlppcp = null;
        IMetadataExtension tupleTranslatorProviderExtension = null;
        mdExtensions = new ArrayList<>();
        MetadataIndexesProvider mdIndexesProvider = new MetadataIndexesProvider(usingDatabase);
        if (list != null) {
            for (AsterixExtension extensionConf : list) {
                IExtension extension = (IExtension) Class.forName(extensionConf.getClassName()).newInstance();
                extension.configure(extensionConf.getArgs());
                switch (extension.getExtensionKind()) {
                    case LANG:
                        ILangExtension le = (ILangExtension) extension;
                        sqlppcp = ExtensionUtil.extendLangCompilationProvider(ILangExtension.Language.SQLPP, sqlppcp,
                                le, namespaceResolver);
                        break;
                    case METADATA:
                        IMetadataExtension mde = (IMetadataExtension) extension;
                        mdExtensions.add(mde);
                        //TODO(DB) clean up
                        tupleTranslatorProviderExtension = ExtensionUtil.extendTupleTranslatorProvider(
                                tupleTranslatorProviderExtension, mde, mdIndexesProvider);
                        break;
                    default:
                        break;
                }
            }
        }
        this.sqlppCompilationProvider =
                sqlppcp == null ? new SqlppCompilationProvider(namespaceResolver) : sqlppcp.second;
        if (tupleTranslatorProviderExtension == null) {
            this.metadataIndexesProvider = mdIndexesProvider;
            this.tupleTranslatorProvider = new MetadataTupleTranslatorProvider(metadataIndexesProvider);
        } else {
            this.metadataIndexesProvider = tupleTranslatorProviderExtension.getMetadataIndexesProvider(usingDatabase);
            this.tupleTranslatorProvider =
                    tupleTranslatorProviderExtension.getMetadataTupleTranslatorProvider(metadataIndexesProvider);
        }
    }

    public ILangCompilationProvider getCompilationProvider(ILangExtension.Language lang) {
        switch (lang) {
            case SQLPP:
                return sqlppCompilationProvider;
            default:
                throw new IllegalArgumentException(String.valueOf(lang));
        }
    }

    public List<IMetadataExtension> getMetadataExtensions() {
        return mdExtensions;
    }

    @Override
    public MetadataTupleTranslatorProvider getMetadataTupleTranslatorProvider() {
        return tupleTranslatorProvider;
    }

    @Override
    public MetadataIndexesProvider getMetadataIndexesProvider() {
        return metadataIndexesProvider;
    }

    /**
     * Called on bootstrap of metadata node allowing extensions to instantiate their Metadata artifacts
     *
     * @param ncServiceCtx
     *         the node controller service context
     * @param mdIndexesProvider
     * @throws HyracksDataException
     */
    public void initializeMetadata(INCServiceContext ncServiceCtx, MetadataIndexesProvider mdIndexesProvider)
            throws HyracksDataException {
        if (mdExtensions != null) {
            for (IMetadataExtension mdExtension : mdExtensions) {
                try {
                    mdExtension.initializeMetadata(ncServiceCtx, mdIndexesProvider);
                } catch (RemoteException | ACIDException e) {
                    throw HyracksDataException.create(e);
                }
            }
        }
    }
}
