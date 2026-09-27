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
package org.apache.asterix.external.input.record.reader.iceberg;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.asterix.external.util.aws.EnsureCloseClientsFactoryRegistry;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;

/**
 * Everything needed to open a table's {@link FileIO} without its catalog: the implementation, its properties, and any
 * storage credentials the catalog vended. Captured from the table loaded at compile time and shipped to the readers,
 * so a reader can open the data files without contacting the catalog again.
 * <p>
 * Iceberg's {@code SerializableTable} is not used for this because it reloads the table metadata through the shipped
 * FileIO the first time a snapshot or schema is asked for, and that FileIO registers its clients under the compile-time
 * registry id: every copy would share the id, and closing any one of them would shut down the clients of all of them.
 */
final class IcebergFileIoDescriptor implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String implementation;
    private final HashMap<String, String> properties;
    private final ArrayList<StorageCredential> credentials;

    private IcebergFileIoDescriptor(String implementation, Map<String, String> properties,
            List<StorageCredential> credentials) {
        this.implementation = implementation;
        this.properties = new HashMap<>(properties);
        this.credentials = new ArrayList<>(credentials);
    }

    /**
     * @return the descriptor of {@code io}, or {@code null} when that FileIO does not expose its properties, in which
     *         case readers have to load the table from the catalog themselves
     */
    static IcebergFileIoDescriptor capture(FileIO io) {
        Map<String, String> properties;
        try {
            properties = io.properties();
        } catch (UnsupportedOperationException e) {
            return null;
        }
        List<StorageCredential> credentials =
                io instanceof SupportsStorageCredentials withCredentials ? withCredentials.credentials() : List.of();
        return new IcebergFileIoDescriptor(io.getClass().getName(), properties, credentials);
    }

    /**
     * @return the properties to open a new FileIO with, carrying a registry id of their own so that the clients the
     *         FileIO creates can be released without touching those of any other reader
     */
    Map<String, String> newProperties() {
        Map<String, String> copy = new HashMap<>(properties);
        copy.put(EnsureCloseClientsFactoryRegistry.FACTORY_INSTANCE_ID_KEY, UUID.randomUUID().toString());
        return copy;
    }

    /**
     * Opens a FileIO with properties obtained from {@link #newProperties()}; the caller owns it and must close it, and
     * then release the registry id in those properties.
     */
    FileIO open(Map<String, String> ioProperties) {
        return CatalogUtil.loadFileIO(implementation, ioProperties, null, credentials);
    }
}
