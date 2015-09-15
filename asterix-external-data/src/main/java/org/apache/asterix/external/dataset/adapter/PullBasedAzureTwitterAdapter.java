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
package org.apache.asterix.external.dataset.adapter;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.feeds.api.IDatasourceAdapter;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.context.IHyracksTaskContext;

public class PullBasedAzureTwitterAdapter extends PullBasedAdapter implements IDatasourceAdapter {
    private static final Logger LOGGER = Logger.getLogger(PullBasedAzureTwitterAdapter.class.getName());

    private static final long serialVersionUID = 1L;

    private final CloudStorageAccount csa;
    private final String connectionString;
    private final String azureAccountName;
    private final String azureAccountKey;
    private final ARecordType outputType;
    private final String tableName;
    private final boolean partitioned;

    private String[] lowKeys;
    private String[] highKeys;

    public PullBasedAzureTwitterAdapter(String accountName, String accountKey, String tableName, String[] partitions,
            Map<String, String> configuration, IHyracksTaskContext ctx, ARecordType outputType) throws AsterixException {
        super(configuration, ctx);
        this.outputType = outputType;
        if (partitions != null) {
            partitioned = true;
            configurePartitions(partitions);
        } else {
            partitioned = false;
        }
        this.azureAccountName = accountName;
        this.azureAccountKey = accountKey;
        this.tableName = tableName;

        connectionString = "DefaultEndpointsProtocol=http;" + "AccountName=" + azureAccountName + ";AccountKey="
                + azureAccountKey + ";";
        try {
            csa = CloudStorageAccount.parse(connectionString);
        } catch (InvalidKeyException | URISyntaxException e) {
            throw new AsterixException("You must specify a valid Azure account name and key", e);
        }
    }

    private void configurePartitions(String[] partitions) {
        lowKeys = new String[partitions.length];
        highKeys = new String[partitions.length];
        for (int i = 0; i < partitions.length; ++i) {
            String[] loHi = partitions[i].split(":");
            lowKeys[i] = loHi[0];
            highKeys[i] = loHi[1];
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Partition " + i + " configured for keys " + lowKeys[i] + " to " + highKeys[i]);
            }
        }
    }

    @Override
    public IPullBasedFeedClient getFeedClient(int partition) throws Exception {
        if (partitioned) {
            return new PullBasedAzureFeedClient(csa, outputType, tableName, lowKeys[partition], highKeys[partition]);
        }
        return new PullBasedAzureFeedClient(csa, outputType, tableName, null, null);
    }

    @Override
    public DataExchangeMode getDataExchangeMode() {
        return DataExchangeMode.PULL;
    }

    @Override
    public boolean handleException(Exception e) {
        return false;
    }
}
