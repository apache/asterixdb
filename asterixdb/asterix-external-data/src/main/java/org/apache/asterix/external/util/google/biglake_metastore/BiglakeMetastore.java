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
package org.apache.asterix.external.util.google.biglake_metastore;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.awsclient.EnsureCloseAWSClientFactory;
import org.apache.asterix.external.util.iceberg.IcebergConstants;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BiglakeMetastore {
    private static final Logger LOGGER = LogManager.getLogger();

    private BiglakeMetastore() {
        throw new AssertionError("do not instantiate");
    }

    public static Catalog initializeCatalog(Map<String, String> catalogProperties, String namespace)
            throws CompilationException {
        // using uuid for a custom catalog name, this not the real catalog name and not used by services
        // it is used by iceberg internally for logging and scoping in multi-catalog environment
        GlueCatalog catalog = new GlueCatalog();
        try {
            catalogProperties.put(CatalogProperties.FILE_IO_IMPL, IcebergConstants.Aws.S3_FILE_IO);
            catalogProperties.put(AwsProperties.CLIENT_FACTORY, EnsureCloseAWSClientFactory.class.getName());

            String catalogName = UUID.randomUUID().toString();
            catalog.initialize(catalogName, catalogProperties);
            LOGGER.debug("Initialized AWS Glue catalog: {}", catalogName);

            if (namespace != null && !catalog.namespaceExists(Namespace.of(namespace))) {
                throw CompilationException.create(ErrorCode.ICEBERG_NAMESPACE_DOES_NOT_EXIST, namespace);
            }
        } catch (CompilationException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw CompilationException.create(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, ex.getMessage());
        }
        return catalog;
    }

    public static void closeCatalog(GlueCatalog catalog) throws CompilationException {
        try {
            if (catalog != null) {
                catalog.close();
            }
        } catch (IOException ex) {
            throw CompilationException.create(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, ex.getMessage());
        }
    }
}
