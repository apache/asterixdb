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

import org.apache.asterix.common.api.IExtension;
import org.apache.asterix.common.config.AsterixExtension;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.metadata.api.IMetadataExtension;
import org.apache.asterix.metadata.entitytupletranslators.MetadataTupleTranslatorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * AsterixDB's implementation of {@code INCExtensionManager} which takes care of
 * initializing extensions on Node Controllers
 */
public class NCExtensionManager {

    private final MetadataTupleTranslatorProvider tupleTranslatorProvider;
    private final List<IMetadataExtension> mdExtensions;

    /**
     * Initialize {@code CCExtensionManager} from configuration
     *
     * @param list
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws ClassNotFoundException
     * @throws HyracksDataException
     */
    public NCExtensionManager(List<AsterixExtension> list)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException, HyracksDataException {
        MetadataTupleTranslatorProvider ttp = null;
        IMetadataExtension tupleTranslatorExtension = null;
        mdExtensions = new ArrayList<>();
        if (list != null) {
            for (AsterixExtension extensionConf : list) {
                IExtension extension = (IExtension) Class.forName(extensionConf.getClassName()).newInstance();
                extension.configure(extensionConf.getArgs());
                switch (extension.getExtensionKind()) {
                    case METADATA:
                        IMetadataExtension mde = (IMetadataExtension) extension;
                        mdExtensions.add(mde);
                        ttp = extendTupleTranslator(ttp, tupleTranslatorExtension, mde);
                        tupleTranslatorExtension = ttp == null ? null : mde;
                        break;
                    default:
                        break;
                }
            }
        }
        this.tupleTranslatorProvider = ttp == null ? new MetadataTupleTranslatorProvider() : ttp;
    }

    private MetadataTupleTranslatorProvider extendTupleTranslator(MetadataTupleTranslatorProvider ttp,
            IMetadataExtension tupleTranslatorExtension, IMetadataExtension mde) throws HyracksDataException {
        if (ttp != null) {
            throw new RuntimeDataException(ErrorCode.ERROR_EXTENSION_COMPONENT_CONFLICT,
                    tupleTranslatorExtension.getId(),
                    mde.getId(), IMetadataExtension.class.getSimpleName());
        }
        return mde.getMetadataTupleTranslatorProvider();
    }

    public List<IMetadataExtension> getMetadataExtensions() {
        return mdExtensions;
    }

    public MetadataTupleTranslatorProvider getMetadataTupleTranslatorProvider() {
        return tupleTranslatorProvider;
    }

    /**
     * Called on bootstrap of metadata node allowing extensions to instantiate their Metadata artifacts
     *
     * @throws HyracksDataException
     */
    public void initializeMetadata() throws HyracksDataException {
        if (mdExtensions != null) {
            for (IMetadataExtension mdExtension : mdExtensions) {
                try {
                    mdExtension.initializeMetadata();
                } catch (RemoteException | ACIDException e) {
                    throw new HyracksDataException(e);
                }
            }
        }
    }
}
