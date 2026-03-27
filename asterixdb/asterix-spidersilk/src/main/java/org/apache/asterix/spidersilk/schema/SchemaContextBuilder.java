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
package org.apache.asterix.spidersilk.schema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.MetadataConstants;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.spidersilk.api.Nl2SqlException;
import org.apache.asterix.spidersilk.api.SchemaContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Builds a {@link SchemaContext} by reading Dataset and type metadata from
 * AsterixDB's {@link MetadataManager}.
 *
 * <p>For each Dataset in the target Dataverse, this class:
 * <ol>
 *   <li>Fetches the ADM item type via {@code MetadataManager.INSTANCE.getDatatype()}</li>
 *   <li>Recursively formats the type tree into a human-readable field list using
 *       {@link DatasetSchemaFormatter}</li>
 *   <li>Marks primary-key fields from {@code Dataset.getPrimaryKeys()}</li>
 *   <li>Returns a {@link SchemaContext} whose {@code datasetDescriptions} list is
 *       ready to be consumed by {@link SchemaEmbeddingService} (PR-3)</li>
 * </ol>
 *
 * <p>All metadata reads are wrapped in a single metadata transaction that is
 * committed on success and aborted on any failure, following the standard
 * AsterixDB metadata access pattern.
 *
 * <p>Example output description for one Dataset:
 * <pre>
 * Dataset TweetMessages (tweetid: bigint [PK], sender-location: point,
 *     send-time: datetime, referred-topics: [string], message-text: string, author-id: bigint)
 * </pre>
 */
public class SchemaContextBuilder {

    private static final Logger LOGGER = LogManager.getLogger();

    private final DatasetSchemaFormatter formatter;
    private final String databaseName;

    /**
     * Creates a builder that reads from the default AsterixDB database
     * ({@code MetadataConstants.DEFAULT_DATABASE}).
     */
    public SchemaContextBuilder() {
        this(MetadataConstants.DEFAULT_DATABASE);
    }

    /**
     * Creates a builder that reads from the specified database.
     *
     * @param databaseName the AsterixDB database name (typically {@code "Default"})
     */
    public SchemaContextBuilder(String databaseName) {
        this.databaseName = databaseName;
        this.formatter = new DatasetSchemaFormatter();
    }

    /**
     * Builds a {@link SchemaContext} containing descriptions for all Datasets
     * in the given Dataverse.
     *
     * @param dataverse the target Dataverse name (e.g. {@code "TinySocial"})
     * @return a populated {@link SchemaContext} ready for embedding and prompt injection
     * @throws Nl2SqlException if the Dataverse does not exist or metadata access fails
     */
    public SchemaContext build(String dataverse) throws Nl2SqlException {
        MetadataTransactionContext txnCtx = null;
        try {
            DataverseName dataverseName = DataverseName.createSinglePartName(dataverse);
            txnCtx = MetadataManager.INSTANCE.beginTransaction();

            List<Dataset> datasets = MetadataManager.INSTANCE.getDataverseDatasets(txnCtx, databaseName, dataverseName);

            if (datasets.isEmpty()) {
                LOGGER.warn("No datasets found in dataverse '{}'", dataverse);
            }

            List<String> descriptions = new ArrayList<>(datasets.size());
            for (Dataset dataset : datasets) {
                try {
                    DatasetSchema schema = buildDatasetSchema(txnCtx, dataset);
                    descriptions.add(schema.toDescriptionString());
                } catch (Exception e) {
                    // Skip datasets whose type cannot be resolved rather than failing the whole request
                    LOGGER.warn("Skipping dataset '{}': failed to resolve type — {}", dataset.getDatasetName(),
                            e.getMessage());
                }
            }

            MetadataManager.INSTANCE.commitTransaction(txnCtx);
            txnCtx = null;
            return new SchemaContext(dataverse, descriptions);

        } catch (Exception e) {
            throw new Nl2SqlException("Failed to build schema context for dataverse: " + dataverse, e);
        } finally {
            if (txnCtx != null) {
                try {
                    MetadataManager.INSTANCE.abortTransaction(txnCtx);
                } catch (Exception ignored) {
                    LOGGER.warn("Failed to abort metadata transaction", ignored);
                }
            }
        }
    }

    /**
     * Builds a {@link DatasetSchema} for a single Dataset by resolving its item type
     * and extracting field metadata.
     */
    private DatasetSchema buildDatasetSchema(MetadataTransactionContext txnCtx, Dataset dataset) throws Exception {
        // Collect primary-key field names for annotation
        Set<String> primaryKeyFields = extractPrimaryKeyFields(dataset);

        // Resolve the ADM item type
        IAType itemType = MetadataManager.INSTANCE.getDatatype(txnCtx, dataset.getItemTypeDatabaseName(),
                dataset.getItemTypeDataverseName(), dataset.getItemTypeName()).getDatatype();

        List<ColumnInfo> columns = new ArrayList<>();
        if (itemType instanceof ARecordType) {
            ARecordType recordType = (ARecordType) itemType;
            String[] fieldNames = recordType.getFieldNames();
            IAType[] fieldTypes = recordType.getFieldTypes();
            for (int i = 0; i < fieldNames.length; i++) {
                String typeStr = formatter.formatType(fieldTypes[i]);
                boolean isPk = primaryKeyFields.contains(fieldNames[i]);
                columns.add(new ColumnInfo(fieldNames[i], typeStr, isPk));
            }
        } else {
            LOGGER.debug("Dataset '{}' has non-record item type: {}", dataset.getDatasetName(), itemType.getTypeTag());
        }

        return new DatasetSchema(dataset.getDatasetName(), columns);
    }

    /**
     * Returns the set of top-level field names that form the primary key.
     * For composite keys only the first part of each key path is collected,
     * which is sufficient for annotation purposes in prompt generation.
     */
    private Set<String> extractPrimaryKeyFields(Dataset dataset) {
        Set<String> pkFields = new HashSet<>();
        List<List<String>> primaryKeys = dataset.getPrimaryKeys();
        for (List<String> keyPath : primaryKeys) {
            if (!keyPath.isEmpty()) {
                pkFields.add(keyPath.get(0));
            }
        }
        return pkFields;
    }
}
