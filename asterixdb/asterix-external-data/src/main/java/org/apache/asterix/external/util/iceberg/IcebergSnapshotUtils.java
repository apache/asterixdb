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

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import static org.apache.asterix.common.exceptions.ErrorCode.EXTERNAL_SOURCE_ERROR;
import static org.apache.asterix.common.exceptions.ErrorCode.PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_SNAPSHOT_ID_PROPERTY_KEY;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_SNAPSHOT_TIMESTAMP_PROPERTY_KEY;
import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Optional;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.util.SnapshotUtil;

public class IcebergSnapshotUtils {

    public static Optional<Long> validateAndGetSnapshot(Map<String, String> properties) throws CompilationException {
        String snapshotId = properties.get(ICEBERG_SNAPSHOT_ID_PROPERTY_KEY);
        String snapshotTimestamp = properties.get(ICEBERG_SNAPSHOT_TIMESTAMP_PROPERTY_KEY);
        if (snapshotId != null && snapshotTimestamp != null) {
            throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT,
                    ICEBERG_SNAPSHOT_TIMESTAMP_PROPERTY_KEY, ICEBERG_SNAPSHOT_ID_PROPERTY_KEY);
        }

        try {
            if (snapshotId != null) {
                return Optional.of(Long.parseLong(snapshotId));
            } else if (snapshotTimestamp != null) {
                return Optional.of(parseTimestamp(snapshotTimestamp));
            } else {
                return Optional.empty();
            }
        } catch (NumberFormatException | DateTimeParseException e) {
            throw new CompilationException(ErrorCode.INVALID_ICEBERG_SNAPSHOT_VALUE,
                    snapshotId != null ? snapshotId : snapshotTimestamp);
        }
    }

    private static long parseTimestamp(String timestamp) throws CompilationException {
        try {
            // try parsing as a long first
            return Long.parseLong(timestamp);
        } catch (NumberFormatException ignored) {
        }

        try {
            // try parsing as ISO 8601 date, e.g., "yyyy-MM-dd"
            return LocalDate.parse(timestamp, ISO_LOCAL_DATE).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        } catch (DateTimeParseException ignored) {
        }

        try {
            // try parsing as ISO 8601 timestamp, e.g., "yyyy-MM-dd'T'HH:mm:ss"
            LocalDateTime localDateTime = LocalDateTime.parse(timestamp, ISO_LOCAL_DATE_TIME);
            return localDateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
        } catch (DateTimeParseException ignored) {
            throw CompilationException.create(EXTERNAL_SOURCE_ERROR,
                    "unexpected TIMESTAMP snapshot format. Allow formats are: (milliseconds, yyyy-MM-dd or "
                            + "yyyy-MM-dd'T'HH:mm:ss). Found: " + timestamp);
        }
    }

    /**
     * Returns the snapshot ID from the configuration. If a snapshot timestamp is provided instead, the snapshot ID
     * for that timestamp is returned instead, otherwise, an error is thrown
     *
     * @param properties properties
     * @param table table the snapshot belongs to
     * @return snapshot id if exists
     * @throws CompilationException CompilationException
     */
    public static Optional<Long> getSnapshotId(Map<String, String> properties, Table table)
            throws CompilationException {
        Optional<Long> snapshotOptional = validateAndGetSnapshot(properties);
        if (snapshotOptional.isEmpty()) {
            return snapshotOptional;
        }

        // if we have a snapshot timestamp, get the snapshot id for that timestamp and validate it instead
        if (properties.containsKey(IcebergConstants.ICEBERG_SNAPSHOT_TIMESTAMP_PROPERTY_KEY)) {
            try {
                return Optional.of(SnapshotUtil.snapshotIdAsOfTime(table, snapshotOptional.get()));
            } catch (IllegalArgumentException e) {
                throw CompilationException.create(EXTERNAL_SOURCE_ERROR, e, getMessageOrToString(e));
            }
        }
        return snapshotOptional;
    }

    public static void validateSnapshotExists(Map<String, String> catalogAndCollectionProperties)
            throws AlgebricksException {
        Optional<Long> snapshotOptional = validateAndGetSnapshot(catalogAndCollectionProperties);
        if (snapshotOptional.isPresent()) {
            String namespace = catalogAndCollectionProperties.get(IcebergConstants.ICEBERG_NAMESPACE_PROPERTY_KEY);
            String tableName = catalogAndCollectionProperties.get(IcebergConstants.ICEBERG_TABLE_NAME_PROPERTY_KEY);
            Map<String, String> catalogProperties =
                    IcebergUtils.filterCatalogProperties(catalogAndCollectionProperties);
            Catalog icebergCatalog = IcebergUtils.initializeCatalog(catalogProperties, namespace, true);

            Namespace parsedNamespace = IcebergUtils.parseNamespace(namespace);
            TableIdentifier tableIdentifier = TableIdentifier.of(parsedNamespace, tableName);
            Table table = icebergCatalog.loadTable(tableIdentifier);

            Optional<Long> snapshotIdOptional = getSnapshotId(catalogAndCollectionProperties, table);
            if (snapshotIdOptional.isPresent() && !snapshotIdExists(table, snapshotIdOptional.get())) {
                throw CompilationException.create(ErrorCode.ICEBERG_SNAPSHOT_ID_NOT_FOUND, snapshotIdOptional.get(),
                        tableIdentifier.toString());
            }
        }
    }

    public static boolean snapshotIdExists(Table table, long snapshot) {
        return table.snapshot(snapshot) != null;
    }
}
