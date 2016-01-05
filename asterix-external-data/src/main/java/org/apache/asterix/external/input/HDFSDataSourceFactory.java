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
package org.apache.asterix.external.input;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.asterix.external.api.IIndexibleExternalDataSource;
import org.apache.asterix.external.api.IInputStreamProvider;
import org.apache.asterix.external.api.IInputStreamProviderFactory;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.external.indexing.IndexingScheduler;
import org.apache.asterix.external.input.record.reader.HDFSRecordReader;
import org.apache.asterix.external.input.stream.HDFSInputStreamProvider;
import org.apache.asterix.external.provider.ExternalIndexerProvider;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.external.util.HDFSUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.hdfs.dataflow.ConfFactory;
import org.apache.hyracks.hdfs.dataflow.InputSplitsFactory;
import org.apache.hyracks.hdfs.scheduler.Scheduler;

public class HDFSDataSourceFactory
        implements IInputStreamProviderFactory, IRecordReaderFactory<Object>, IIndexibleExternalDataSource {

    protected static final long serialVersionUID = 1L;
    protected transient AlgebricksPartitionConstraint clusterLocations;
    protected String[] readSchedule;
    protected boolean read[];
    protected InputSplitsFactory inputSplitsFactory;
    protected ConfFactory confFactory;
    protected boolean configured = false;
    protected static Scheduler hdfsScheduler;
    protected static IndexingScheduler indexingScheduler;
    protected static Boolean initialized = false;
    protected List<ExternalFile> files;
    protected Map<String, String> configuration;
    protected Class<?> recordClass;
    protected boolean indexingOp = false;
    private JobConf conf;
    private InputSplit[] inputSplits;
    private String nodeName;

    @Override
    public void configure(Map<String, String> configuration) throws Exception {
        if (!HDFSDataSourceFactory.initialized) {
            HDFSDataSourceFactory.initialize();
        }
        this.configuration = configuration;
        JobConf conf = HDFSUtils.configureHDFSJobConf(configuration);
        confFactory = new ConfFactory(conf);
        clusterLocations = getPartitionConstraint();
        int numPartitions = ((AlgebricksAbsolutePartitionConstraint) clusterLocations).getLocations().length;
        // if files list was set, we restrict the splits to the list
        InputSplit[] inputSplits;
        if (files == null) {
            inputSplits = conf.getInputFormat().getSplits(conf, numPartitions);
        } else {
            inputSplits = HDFSUtils.getSplits(conf, files);
        }
        if (indexingOp) {
            readSchedule = indexingScheduler.getLocationConstraints(inputSplits);
        } else {
            readSchedule = hdfsScheduler.getLocationConstraints(inputSplits);
        }
        inputSplitsFactory = new InputSplitsFactory(inputSplits);
        read = new boolean[readSchedule.length];
        Arrays.fill(read, false);
        if (!ExternalDataUtils.isDataSourceStreamProvider(configuration)) {
            RecordReader<?, ?> reader = conf.getInputFormat().getRecordReader(inputSplits[0], conf, Reporter.NULL);
            this.recordClass = reader.createValue().getClass();
            reader.close();
        }
    }

    // Used to tell the factory to restrict the splits to the intersection between this list and the actual files on hdfs side
    @Override
    public void setSnapshot(List<ExternalFile> files, boolean indexingOp) {
        this.files = files;
        this.indexingOp = indexingOp;
    }

    /*
     * The method below was modified to take care of the following
     * 1. when target files are not null, it generates a file aware input stream that validate against the files
     * 2. if the data is binary, it returns a generic reader
     */
    @Override
    public IInputStreamProvider createInputStreamProvider(IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        try {
            if (!configured) {
                conf = confFactory.getConf();
                inputSplits = inputSplitsFactory.getSplits();
                nodeName = ctx.getJobletContext().getApplicationContext().getNodeId();
                configured = true;
            }
            return new HDFSInputStreamProvider<Object>(read, inputSplits, readSchedule, nodeName, conf, configuration,
                    files);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    /**
     * Get the cluster locations for this input stream factory. This method specifies on which asterix nodes the
     * external
     * adapter will run and how many threads per node.
     * @return
     */
    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() {
        clusterLocations = HDFSUtils.getPartitionConstraints(clusterLocations);
        return clusterLocations;
    }

    /**
     * This method initialize the scheduler which assigns responsibility of reading different logical input splits from
     * HDFS
     */
    private static void initialize() {
        synchronized (initialized) {
            if (!initialized) {
                hdfsScheduler = HDFSUtils.initializeHDFSScheduler();
                indexingScheduler = HDFSUtils.initializeIndexingHDFSScheduler();
                initialized = true;
            }
        }
    }

    public JobConf getJobConf() throws HyracksDataException {
        return confFactory.getConf();
    }

    @Override
    public DataSourceType getDataSourceType() {
        return (ExternalDataUtils.isDataSourceStreamProvider(configuration)) ? DataSourceType.STREAM
                : DataSourceType.RECORDS;
    }

    @Override
    public IRecordReader<? extends Writable> createRecordReader(IHyracksTaskContext ctx, int partition)
            throws Exception {
        JobConf conf = confFactory.getConf();
        InputSplit[] inputSplits = inputSplitsFactory.getSplits();
        String nodeName = ctx.getJobletContext().getApplicationContext().getNodeId();
        HDFSRecordReader<Object, Writable> recordReader = new HDFSRecordReader<Object, Writable>(read, inputSplits,
                readSchedule, nodeName, conf);
        if (files != null) {
            recordReader.setSnapshot(files);
            recordReader.setIndexer(ExternalIndexerProvider.getIndexer(configuration));
        }
        recordReader.configure(configuration);
        return recordReader;
    }

    @Override
    public Class<?> getRecordClass() {
        return recordClass;
    }

    @Override
    public boolean isIndexible() {
        return true;
    }

    @Override
    public boolean isIndexingOp() {
        return (files != null && indexingOp);
    }
}
