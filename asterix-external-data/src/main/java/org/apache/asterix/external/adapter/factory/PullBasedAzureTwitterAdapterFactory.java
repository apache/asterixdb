/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.external.adapter.factory;

import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.FeedPolicyAccessor;
import edu.uci.ics.asterix.common.feeds.api.IDatasourceAdapter;
import edu.uci.ics.asterix.common.feeds.api.IIntakeProgressTracker;
import edu.uci.ics.asterix.external.dataset.adapter.PullBasedAzureTwitterAdapter;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.entities.Datatype;
import edu.uci.ics.asterix.metadata.feeds.IFeedAdapterFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public class PullBasedAzureTwitterAdapterFactory implements IFeedAdapterFactory {

    private static final long serialVersionUID = 1L;

    private static final String INGESTOR_LOCATIONS_KEY = "ingestor-locations";
    private static final String PARTITIONS_KEY = "partitions";
    private static final String OUTPUT_TYPE_KEY = "output-type";
    private static final String TABLE_NAME_KEY = "table-name";
    private static final String ACCOUNT_NAME_KEY = "account-name";
    private static final String ACCOUNT_KEY_KEY = "account-key";

    private ARecordType outputType;
    private Map<String, String> configuration;
    private String tableName;
    private String azureAccountName;
    private String azureAccountKey;
    private String[] locations;
    private String[] partitions;
    private FeedPolicyAccessor ingestionPolicy;

    @Override
    public SupportedOperation getSupportedOperations() {
        return SupportedOperation.READ;
    }

    @Override
    public String getName() {
        return "azure_twitter";
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        String locationsStr = configuration.get(INGESTOR_LOCATIONS_KEY);
        if (locationsStr == null) {
            return null;
        }
        String[] locations = locationsStr.split(",");
        return new AlgebricksAbsolutePartitionConstraint(locations);
    }

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws Exception {
        return new PullBasedAzureTwitterAdapter(azureAccountName, azureAccountKey, tableName, partitions,
                configuration, ctx, outputType);
    }

    @Override
    public ARecordType getAdapterOutputType() {
        return outputType;
    }

    @Override
    public void configure(Map<String, String> configuration, ARecordType outputType) throws Exception {
        this.configuration = configuration;
        this.outputType = outputType;

        tableName = configuration.get(TABLE_NAME_KEY);
        if (tableName == null) {
            throw new AsterixException("You must specify a valid table name");
        }
        azureAccountName = configuration.get(ACCOUNT_NAME_KEY);
        azureAccountKey = configuration.get(ACCOUNT_KEY_KEY);
        if (azureAccountName == null || azureAccountKey == null) {
            throw new AsterixException("You must specify a valid Azure account name and key");
        }

        int nIngestLocations = 1;
        String locationsStr = configuration.get(INGESTOR_LOCATIONS_KEY);
        if (locationsStr != null) {
            locations = locationsStr.split(",");
            nIngestLocations = locations.length;
        }

        int nPartitions = 1;
        String partitionsStr = configuration.get(PARTITIONS_KEY);
        if (partitionsStr != null) {
            partitions = partitionsStr.split(",");
            nPartitions = partitions.length;
        }

        if (nIngestLocations != nPartitions) {
            throw new AsterixException("Invalid adapter configuration: number of ingestion-locations ("
                    + nIngestLocations + ") must be the same as the number of partitions (" + nPartitions + ")");
        }
        configureType();
    }

    private void configureType() throws Exception {
        String fqOutputType = configuration.get(OUTPUT_TYPE_KEY);

        if (fqOutputType == null) {
            throw new IllegalArgumentException("No output type specified");
        }
        String[] dataverseAndType = fqOutputType.split("[.]");
        String dataverseName = dataverseAndType[0];
        String datatypeName = dataverseAndType[1];

        MetadataTransactionContext ctx = null;
        MetadataManager.INSTANCE.acquireReadLatch();
        try {
            ctx = MetadataManager.INSTANCE.beginTransaction();
            Datatype t = MetadataManager.INSTANCE.getDatatype(ctx, dataverseName, datatypeName);
            IAType type = t.getDatatype();
            if (type.getTypeTag() != ATypeTag.RECORD) {
                throw new IllegalStateException();
            }
            outputType = (ARecordType) t.getDatatype();
            MetadataManager.INSTANCE.commitTransaction(ctx);
        } catch (Exception e) {
            if (ctx != null) {
                MetadataManager.INSTANCE.abortTransaction(ctx);
            }
            throw e;
        } finally {
            MetadataManager.INSTANCE.releaseReadLatch();
        }
    }

    @Override
    public boolean isRecordTrackingEnabled() {
        return false;
    }

    @Override
    public IIntakeProgressTracker createIntakeProgressTracker() {
        return null;
    }

    public FeedPolicyAccessor getIngestionPolicy() {
        return ingestionPolicy;
    }
    
    

}
