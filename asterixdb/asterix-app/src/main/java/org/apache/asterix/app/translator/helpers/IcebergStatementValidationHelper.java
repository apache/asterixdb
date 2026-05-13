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
package org.apache.asterix.app.translator.helpers;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.iceberg.IcebergConstants;
import org.apache.asterix.external.util.iceberg.IcebergSnapshotUtils;
import org.apache.asterix.external.util.iceberg.IcebergUtils;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Catalog;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class IcebergStatementValidationHelper {

    public IcebergStatementValidationHelper() {
    }

    public static void validateIfIcebergTable(ICcApplicationContext appCtx, MetadataProvider metadataProvider,
            MetadataTransactionContext mdTxnCtx, Map<String, String> collectionProperties, SourceLocation srcLoc,
            String adapter) throws AlgebricksException {
        validateIfIcebergTable(appCtx, metadataProvider, mdTxnCtx, collectionProperties, Collections.emptyMap(), srcLoc,
                adapter);
    }

    public static void validateIfIcebergTable(ICcApplicationContext appCtx, MetadataProvider metadataProvider,
            MetadataTransactionContext mdTxnCtx, Map<String, String> collectionProperties,
            Map<String, String> extraCollectionProperties, SourceLocation srcLoc, String adapter)
            throws AlgebricksException {
        if (!IcebergUtils.isIcebergTable(collectionProperties)) {
            return;
        }
        IcebergUtils.setDefaultFormat(collectionProperties);
        IcebergUtils.validateIcebergTableProperties(collectionProperties);

        // work on a copy of the properties from now onward to avoid modifying the original collection properties
        Map<String, String> propertiesCopy = new HashMap<>(collectionProperties);
        propertiesCopy.put(ExternalDataConstants.KEY_EXTERNAL_SOURCE_TYPE, adapter);

        // ensure the specified catalog exists
        String catalogName = propertiesCopy.get(IcebergConstants.ICEBERG_CATALOG_NAME);
        Catalog catalog = MetadataManager.INSTANCE.getCatalog(mdTxnCtx, catalogName);
        if (catalog == null) {
            throw new CompilationException(ErrorCode.UNKNOWN_CATALOG, srcLoc, catalogName);
        }

        // validate snapshot exists if provided
        propertiesCopy.putAll(extraCollectionProperties);
        metadataProvider.addIcebergCatalogPropertiesIfNeeded(appCtx, propertiesCopy);
        IcebergSnapshotUtils.validateSnapshotExists(propertiesCopy);
    }
}
