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
package org.apache.asterix.external.util.iceberg;

import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_TABLE_FORMAT;

import java.util.Map;

import org.apache.asterix.common.config.CatalogConfig;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.util.ExternalDataConstants;

public class IcebergUtils {

    /**
     * Checks if the provided catalog is an Iceberg catalog
     *
     * @param catalogType catalog type
     * @return true if Iceberg catalog, false otherwise
     */
    public static boolean isIcebergCatalog(String catalogType) {
        if (catalogType == null) {
            return false;
        }
        return catalogType.equalsIgnoreCase(CatalogConfig.CatalogType.ICEBERG.name());
    }

    /**
     * Checks if the provided configuration is for Iceberg table
     *
     * @param configuration external data configuration
     */
    public static boolean isIcebergTable(Map<String, String> configuration) {
        String tableFormat = configuration.get(ExternalDataConstants.TABLE_FORMAT);
        if (tableFormat != null) {
            return tableFormat.equals(ICEBERG_TABLE_FORMAT);
        }
        return false;
    }

    public static void validateCatalogProperties(Map<String, String> properties) throws CompilationException {
        String catalogSource = properties.get(IcebergConstants.ICEBERG_SOURCE_PROPERTY_KEY);
        if (catalogSource == null) {
            throw new CompilationException(ErrorCode.ICEBERG_CATALOG_SOURCE_REQUIRED);
        }
        validateCatalogSource(catalogSource);
        validatePropertyPresent(properties, IcebergConstants.ICEBERG_SOURCE_PROPERTY_KEY);
        validatePropertyPresent(properties, IcebergConstants.ICEBERG_WAREHOUSE_PROPERTY_KEY);
    }

    /**
     * Checks if the provided iceberg catalog source is a supported iceberg catalog type
     *
     * @param catalogSource catalog source
     * @throws CompilationException CompilationException
     */
    public static void validateCatalogSource(String catalogSource) throws CompilationException {
        for (CatalogConfig.IcebergCatalogSource icebergSource : CatalogConfig.IcebergCatalogSource.values()) {
            if (icebergSource.name().equalsIgnoreCase(catalogSource)) {
                return;
            }
        }
        throw new CompilationException(ErrorCode.UNSUPPORTED_ICEBERG_CATALOG_SOURCE, catalogSource);
    }

    private static void validatePropertyPresent(Map<String, String> properties, String key)
            throws CompilationException {
        String property = properties.get(key);
        if (property == null || property.isEmpty()) {
            throw new CompilationException(ErrorCode.PARAMETERS_REQUIRED, key);
        }
    }
}
