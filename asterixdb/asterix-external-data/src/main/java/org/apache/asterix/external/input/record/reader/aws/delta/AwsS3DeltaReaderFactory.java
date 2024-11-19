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
package org.apache.asterix.external.input.record.reader.aws.delta;

import static org.apache.asterix.external.util.aws.s3.S3Constants.SERVICE_END_POINT_FIELD_NAME;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.external.IExternalFilterEvaluatorFactory;
import org.apache.asterix.external.api.IExternalDataRuntimeContext;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.aws.s3.S3Constants;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterator;

public class AwsS3DeltaReaderFactory implements IRecordReaderFactory<Object> {

    private static final long serialVersionUID = 1L;
    private static final List<String> recordReaderNames =
            Collections.singletonList(ExternalDataConstants.KEY_ADAPTER_NAME_AWS_S3);
    private static final Logger LOGGER = LogManager.getLogger();
    private transient AlgebricksAbsolutePartitionConstraint locationConstraints;
    private Map<Integer, List<String>> schedule;
    private String scanState;
    private Map<String, String> configuration;
    private List<String> scanFiles;

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint() {
        return locationConstraints;
    }

    @Override
    public void configure(IServiceContext serviceCtx, Map<String, String> configuration,
            IWarningCollector warningCollector, IExternalFilterEvaluatorFactory filterEvaluatorFactory)
            throws AlgebricksException, HyracksDataException {
        this.configuration = configuration;
        Configuration conf = new Configuration();
        conf.set(S3Constants.HADOOP_ACCESS_KEY_ID, configuration.get(S3Constants.ACCESS_KEY_ID_FIELD_NAME));
        conf.set(S3Constants.HADOOP_SECRET_ACCESS_KEY, configuration.get(S3Constants.SECRET_ACCESS_KEY_FIELD_NAME));
        if (configuration.get(S3Constants.SESSION_TOKEN_FIELD_NAME) != null) {
            conf.set(S3Constants.HADOOP_SESSION_TOKEN, configuration.get(S3Constants.SESSION_TOKEN_FIELD_NAME));
        }
        conf.set(S3Constants.HADOOP_REGION, configuration.get(S3Constants.REGION_FIELD_NAME));
        String serviceEndpoint = configuration.get(SERVICE_END_POINT_FIELD_NAME);
        if (serviceEndpoint != null) {
            conf.set(S3Constants.HADOOP_SERVICE_END_POINT, serviceEndpoint);
        }
        String tableMetadataPath = S3Constants.HADOOP_S3_PROTOCOL + "://"
                + configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME) + '/'
                + configuration.get(ExternalDataConstants.DEFINITION_FIELD_NAME);

        ICcApplicationContext appCtx = (ICcApplicationContext) serviceCtx.getApplicationContext();

        Engine engine = DefaultEngine.create(conf);
        io.delta.kernel.Table table = io.delta.kernel.Table.forPath(engine, tableMetadataPath);
        Snapshot snapshot = table.getLatestSnapshot(engine);
        Scan scan = snapshot.getScanBuilder(engine).withReadSchema(engine, snapshot.getSchema(engine)).build();
        scanState = RowSerDe.serializeRowToJson(scan.getScanState(engine));
        CloseableIterator<FilteredColumnarBatch> iter = scan.getScanFiles(engine);

        scanFiles = new ArrayList<>();
        while (iter.hasNext()) {
            FilteredColumnarBatch batch = iter.next();
            CloseableIterator<Row> rowIter = batch.getRows();
            while (rowIter.hasNext()) {
                Row row = rowIter.next();
                scanFiles.add(RowSerDe.serializeRowToJson(row));
            }
        }
        locationConstraints = configureLocationConstraints(appCtx);
        configuration.put(ExternalDataConstants.KEY_PARSER, ExternalDataConstants.FORMAT_DELTA);
        distributeFiles();
    }

    private AlgebricksAbsolutePartitionConstraint configureLocationConstraints(ICcApplicationContext appCtx) {
        IClusterStateManager csm = appCtx.getClusterStateManager();

        String[] locations = csm.getClusterLocations().getLocations();
        if (scanFiles.size() == 0) {
            return AlgebricksAbsolutePartitionConstraint.randomLocation(locations);
        } else if (locations.length > scanFiles.size()) {
            LOGGER.debug(
                    "analytics partitions ({}) exceeds total partition count ({}); limiting ingestion partitions to total partition count",
                    locations.length, scanFiles.size());
            final String[] locationCopy = locations.clone();
            ArrayUtils.shuffle(locationCopy);
            locations = ArrayUtils.subarray(locationCopy, 0, scanFiles.size());
        }
        return new AlgebricksAbsolutePartitionConstraint(locations);
    }

    private void distributeFiles() {
        final int numComputePartitions = getPartitionConstraint().getLocations().length;
        schedule = new HashMap<>();
        for (int i = 0; i < numComputePartitions; i++) {
            schedule.put(i, new ArrayList<>());
        }
        int i = 0;
        for (String scanFile : scanFiles) {
            schedule.get(i).add(scanFile);
            i = (i + 1) % numComputePartitions;
        }
    }

    @Override
    public IRecordReader<?> createRecordReader(IExternalDataRuntimeContext context) throws HyracksDataException {
        try {
            int partition = context.getPartition();
            return new DeltaFileRecordReader(schedule.get(partition), scanState, configuration, context);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public Class<?> getRecordClass() throws AsterixException {
        return Row.class;
    }

    @Override
    public List<String> getRecordReaderNames() {
        return recordReaderNames;
    }

    @Override
    public Set<String> getReaderSupportedFormats() {
        return Collections.singleton(ExternalDataConstants.FORMAT_DELTA);
    }

}
