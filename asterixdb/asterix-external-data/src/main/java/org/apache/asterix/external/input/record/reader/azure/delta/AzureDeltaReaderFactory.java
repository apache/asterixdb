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
package org.apache.asterix.external.input.record.reader.azure.delta;

import static org.apache.asterix.external.util.azure.AzureConstants.HADOOP_AZURE_PROTOCOL;
import static org.apache.asterix.external.util.azure.AzureUtils.extractEndPoint;
import static org.apache.asterix.external.util.azure.blob.BlobUtils.buildClient;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.external.input.record.reader.aws.delta.DeltaReaderFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.azure.AzureConstants;
import org.apache.asterix.external.util.azure.AzureUtils;
import org.apache.asterix.external.util.azure.datalake.DatalakeUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

import com.azure.storage.file.datalake.DataLakeServiceClient;

public class AzureDeltaReaderFactory extends DeltaReaderFactory {
    private static final long serialVersionUID = 1L;
    private static final List<String> RECORD_READER_NAMES =
            Collections.singletonList(ExternalDataConstants.KEY_ADAPTER_NAME_AZURE_DATA_LAKE);

    @Override
    protected void configureJobConf(IApplicationContext appCtx, JobConf conf, Map<String, String> configuration)
            throws AlgebricksException {
        // get endpoint
        DataLakeServiceClient dataLakeServiceClient = DatalakeUtils.buildClient(appCtx, configuration);
        String endPoint = extractEndPoint(dataLakeServiceClient.getAccountUrl());
        configuration.put(AzureConstants.ACCOUNT_URL, dataLakeServiceClient.getAccountUrl());
        AzureUtils.configureAzureHdfsJobConf(conf, configuration, endPoint);
    }

    @Override
    protected String getTablePath(Map<String, String> configuration) throws AlgebricksException {
        return HADOOP_AZURE_PROTOCOL + "://" + configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME) + '@'
                + extractEndPoint(configuration.get(AzureConstants.ACCOUNT_URL)) + '/'
                + configuration.get(ExternalDataConstants.DEFINITION_FIELD_NAME);
    }

    @Override
    public List<String> getRecordReaderNames() {
        return RECORD_READER_NAMES;
    }

}
