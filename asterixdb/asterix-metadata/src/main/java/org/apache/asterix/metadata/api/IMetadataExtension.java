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

package org.apache.asterix.metadata.api;

import java.rmi.RemoteException;
import java.util.List;

import org.apache.asterix.common.api.IExtension;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.metadata.entitytupletranslators.MetadataTupleTranslatorProvider;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * An interface for Metadata Extensions
 */
public interface IMetadataExtension extends IExtension {

    @Override
    default ExtensionKind getExtensionKind() {
        return ExtensionKind.METADATA;
    }

    /**
     * @return The tuple translator provider that must be used by the {@code IMetadataNode } to read and write core
     *         {@code IMetadataEntity} objects
     */
    MetadataTupleTranslatorProvider getMetadataTupleTranslatorProvider();

    /**
     * @return A list of additional extension instances of {@code IMetadataIndex} that are introduced by the extension
     */
    @SuppressWarnings("rawtypes")
    List<ExtensionMetadataDataset> getExtensionIndexes();

    /**
     * Called when booting the {@code IMetadataNode}
     *
     * @throws HyracksDataException
     * @throws RemoteException
     * @throws ACIDException
     */
    void initializeMetadata(INCServiceContext ncServiceCtx) throws HyracksDataException, RemoteException, ACIDException;

}
