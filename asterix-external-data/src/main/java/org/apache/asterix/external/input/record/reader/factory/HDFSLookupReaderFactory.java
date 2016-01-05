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
package org.apache.asterix.external.input.record.reader.factory;

import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.ILookupReaderFactory;
import org.apache.asterix.external.api.ILookupRecordReader;
import org.apache.asterix.external.indexing.ExternalFileIndexAccessor;
import org.apache.asterix.external.input.record.reader.RCLookupReader;
import org.apache.asterix.external.input.record.reader.SequenceLookupReader;
import org.apache.asterix.external.input.record.reader.TextLookupReader;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.HDFSUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.hdfs.dataflow.ConfFactory;

public class HDFSLookupReaderFactory<T> implements ILookupReaderFactory<T> {

    protected static final long serialVersionUID = 1L;
    protected transient AlgebricksPartitionConstraint clusterLocations;
    protected ConfFactory confFactory;
    protected Map<String, String> configuration;

    public HDFSLookupReaderFactory() {
    }

    @Override
    public DataSourceType getDataSourceType() {
        return DataSourceType.RECORDS;
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        clusterLocations = HDFSUtils.getPartitionConstraints(clusterLocations);
        return clusterLocations;
    }

    @Override
    public void configure(Map<String, String> configuration) throws Exception {
        this.configuration = configuration;
        JobConf conf = HDFSUtils.configureHDFSJobConf(configuration);
        confFactory = new ConfFactory(conf);

    }

    @Override
    public boolean isIndexible() {
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ILookupRecordReader<? extends T> createRecordReader(IHyracksTaskContext ctx, int partition,
            ExternalFileIndexAccessor snapshotAccessor) throws Exception {
        String inputFormatParameter = configuration.get(ExternalDataConstants.KEY_INPUT_FORMAT).trim();
        JobConf conf = confFactory.getConf();
        FileSystem fs = FileSystem.get(conf);
        switch (inputFormatParameter) {
            case ExternalDataConstants.INPUT_FORMAT_TEXT:
                return (ILookupRecordReader<? extends T>) new TextLookupReader(snapshotAccessor, fs, conf);
            case ExternalDataConstants.INPUT_FORMAT_SEQUENCE:
                return (ILookupRecordReader<? extends T>) new SequenceLookupReader(snapshotAccessor, fs, conf);
            case ExternalDataConstants.INPUT_FORMAT_RC:
                return (ILookupRecordReader<? extends T>) new RCLookupReader(snapshotAccessor, fs, conf);
            default:
                throw new AsterixException("Unrecognised input format: " + inputFormatParameter);
        }
    }
}
