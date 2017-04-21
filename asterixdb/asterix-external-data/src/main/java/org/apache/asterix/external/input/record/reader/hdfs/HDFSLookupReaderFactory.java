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
package org.apache.asterix.external.input.record.reader.hdfs;

import java.io.IOException;
import java.util.Map;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.ILookupReaderFactory;
import org.apache.asterix.external.api.ILookupRecordReader;
import org.apache.asterix.external.indexing.ExternalFileIndexAccessor;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.HDFSUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.hdfs.dataflow.ConfFactory;

public class HDFSLookupReaderFactory<T> implements ILookupReaderFactory<T> {

    private static final long serialVersionUID = 1L;
    protected ConfFactory confFactory;
    protected Map<String, String> configuration;
    protected transient AlgebricksAbsolutePartitionConstraint clusterLocations;
    protected transient IServiceContext serviceCtx;

    public HDFSLookupReaderFactory() {
    }

    @Override
    public DataSourceType getDataSourceType() {
        return DataSourceType.RECORDS;
    }

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint() throws AsterixException {
        clusterLocations = HDFSUtils.getPartitionConstraints((IApplicationContext) serviceCtx.getApplicationContext(),
                clusterLocations);
        return clusterLocations;
    }

    @Override
    public void configure(IServiceContext serviceCtx, Map<String, String> configuration) throws AsterixException {
        this.serviceCtx = serviceCtx;
        this.configuration = configuration;
        JobConf conf = HDFSUtils.configureHDFSJobConf(configuration);
        try {
            confFactory = new ConfFactory(conf);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }

    }

    @Override
    public boolean isIndexible() {
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ILookupRecordReader<? extends T> createRecordReader(IHyracksTaskContext ctx, int partition,
            ExternalFileIndexAccessor snapshotAccessor) throws HyracksDataException {
        String inputFormatParameter = configuration.get(ExternalDataConstants.KEY_INPUT_FORMAT).trim();
        JobConf conf = confFactory.getConf();
        FileSystem fs;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            throw new HyracksDataException("Unable to get filesystem object", e);
        }
        switch (inputFormatParameter) {
            case ExternalDataConstants.INPUT_FORMAT_TEXT:
                return (ILookupRecordReader<? extends T>) new TextLookupReader(snapshotAccessor, fs, conf);
            case ExternalDataConstants.INPUT_FORMAT_SEQUENCE:
                return (ILookupRecordReader<? extends T>) new SequenceLookupReader(snapshotAccessor, fs, conf);
            case ExternalDataConstants.INPUT_FORMAT_RC:
                return (ILookupRecordReader<? extends T>) new RCLookupReader(snapshotAccessor, fs, conf);
            default:
                throw new HyracksDataException("Unrecognised input format: " + inputFormatParameter);
        }
    }
}
