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
package org.apache.asterix.external.util.google.iceberg.biglake_metastore;

import static org.apache.asterix.common.exceptions.ErrorCode.PARAMETERS_REQUIRED;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_URI_PROPERTY_KEY;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_WAREHOUSE_PROPERTY_KEY;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.Gcp.QUOTA_PROJECT_ID_HEADER_KEY;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.Gcp.QUOTA_PROJECT_ID_KEY;
import static org.apache.asterix.external.util.iceberg.IcebergUtils.validatePropertyExists;
import static org.apache.iceberg.CatalogProperties.CATALOG_IMPL;
import static org.apache.iceberg.CatalogProperties.URI;
import static org.apache.iceberg.rest.auth.AuthProperties.AUTH_TYPE;

import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.external.util.google.iceberg.auth.InMemoryServiceAccountGoogleAuthManager;
import org.apache.iceberg.rest.RESTCatalog;

public class BiglakeMetastoreUtils {

    private BiglakeMetastoreUtils() {
        throw new AssertionError("do not instantiate");
    }

    public static void setCatalogProperties(Map<String, String> catalogProperties) {
        catalogProperties.put(CATALOG_IMPL, RESTCatalog.class.getName());
        catalogProperties.put(URI, catalogProperties.get(ICEBERG_URI_PROPERTY_KEY));
        catalogProperties.put(AUTH_TYPE, InMemoryServiceAccountGoogleAuthManager.class.getName());
        catalogProperties.put(QUOTA_PROJECT_ID_HEADER_KEY, catalogProperties.get(QUOTA_PROJECT_ID_KEY));
    }

    public static void validateRequiredProperties(Map<String, String> catalogProperties) throws CompilationException {
        validatePropertyExists(catalogProperties, ICEBERG_URI_PROPERTY_KEY, PARAMETERS_REQUIRED);
        validatePropertyExists(catalogProperties, ICEBERG_WAREHOUSE_PROPERTY_KEY, PARAMETERS_REQUIRED);
        validatePropertyExists(catalogProperties, QUOTA_PROJECT_ID_KEY, PARAMETERS_REQUIRED);
    }
}
