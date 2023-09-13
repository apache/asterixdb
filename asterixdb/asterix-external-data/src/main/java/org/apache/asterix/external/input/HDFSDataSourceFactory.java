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

import static org.apache.asterix.external.util.ExternalDataConstants.CONTAINER_NAME_FIELD_NAME;
import static org.apache.asterix.external.util.ExternalDataConstants.FORMAT_PARQUET;
import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.external.IExternalFilterEvaluatorFactory;
import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.api.IExternalDataRuntimeContext;
import org.apache.asterix.external.api.IExternalDataSourceFactory;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.external.input.filter.embedder.IExternalFilterValueEmbedder;
import org.apache.asterix.external.input.record.reader.hdfs.HDFSRecordReader;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.ParquetFileRecordReader;
import org.apache.asterix.external.input.record.reader.stream.StreamRecordReader;
import org.apache.asterix.external.input.stream.HDFSInputStream;
import org.apache.asterix.external.provider.StreamRecordReaderProvider;
import org.apache.asterix.external.provider.context.ExternalStreamRuntimeDataContext;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.external.util.HDFSUtils;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.hdfs.dataflow.ConfFactory;
import org.apache.hyracks.hdfs.dataflow.InputSplitsFactory;
import org.apache.hyracks.hdfs.scheduler.Scheduler;

public class HDFSDataSourceFactory implements IRecordReaderFactory<Object>, IExternalDataSourceFactory {

    private static final long serialVersionUID = 1L;
    private static final List<String> recordReaderNames = Collections.singletonList("hdfs");

    protected transient AlgebricksAbsolutePartitionConstraint clusterLocations;
    protected transient IServiceContext serviceCtx;
    protected String[] readSchedule;
    protected boolean read[];
    protected InputSplitsFactory inputSplitsFactory;
    protected ConfFactory confFactory;
    protected boolean configured = false;
    protected static Scheduler hdfsScheduler;
    protected static Boolean initialized = false;
    protected static Object initLock = new Object();
    protected Map<String, String> configuration;
    protected Class<?> recordClass;
    private JobConf conf;
    private InputSplit[] inputSplits;
    private String nodeName;
    private Class recordReaderClazz;
    private IExternalFilterEvaluatorFactory filterEvaluatorFactory;

    @Override
    public void configure(IServiceContext serviceCtx, Map<String, String> configuration,
            IWarningCollector warningCollector, IExternalFilterEvaluatorFactory filterEvaluatorFactory)
            throws AlgebricksException, HyracksDataException {
        JobConf hdfsConf = prepareHDFSConf(serviceCtx, configuration, filterEvaluatorFactory);
        configureHdfsConf(hdfsConf, configuration);
    }

    protected JobConf prepareHDFSConf(IServiceContext serviceCtx, Map<String, String> configuration,
            IExternalFilterEvaluatorFactory filterEvaluatorFactory) throws HyracksDataException {
        this.serviceCtx = serviceCtx;
        this.configuration = configuration;
        this.filterEvaluatorFactory = filterEvaluatorFactory;
        init((ICCServiceContext) serviceCtx);
        return HDFSUtils.configureHDFSJobConf(configuration);
    }

    protected void configureHdfsConf(JobConf conf, Map<String, String> configuration) throws AlgebricksException {
        String formatString = configuration.get(ExternalDataConstants.KEY_FORMAT);
        try {
            confFactory = new ConfFactory(conf);
            clusterLocations = getPartitionConstraint();
            int numPartitions = clusterLocations.getLocations().length;
            InputSplit[] configInputSplits = getInputSplits(conf, numPartitions);
            readSchedule = hdfsScheduler.getLocationConstraints(configInputSplits);
            inputSplitsFactory = new InputSplitsFactory(configInputSplits);
            read = new boolean[readSchedule.length];
            Arrays.fill(read, false);
            if (formatString == null || formatString.equals(ExternalDataConstants.FORMAT_HDFS_WRITABLE)) {
                RecordReader<?, ?> reader =
                        conf.getInputFormat().getRecordReader(configInputSplits[0], conf, Reporter.NULL);
                this.recordClass = reader.createValue().getClass();
                reader.close();
            } else if (formatString.equals(ExternalDataConstants.FORMAT_PARQUET)) {
                recordClass = IValueReference.class;
            } else {
                recordReaderClazz = StreamRecordReaderProvider.getRecordReaderClazz(configuration);
                this.recordClass = char[].class;
            }
        } catch (IOException e) {
            throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, e, getMessageOrToString(e));
        } catch (Exception e) {
            if (FORMAT_PARQUET.equals(formatString)) {
                String containerName = configuration.get(CONTAINER_NAME_FIELD_NAME);
                if (containerName != null && containerName.contains(".")) {
                    throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, e,
                            getMessageOrToString(e) + " Buckets with '.' in the name can cause issues.");
                } else {
                    throw e;
                }
            } else {
                throw e;
            }
        }
    }

    private InputSplit[] getInputSplits(JobConf conf, int numPartitions) throws IOException {
        if (HDFSUtils.isEmpty(conf)) {
            return Scheduler.EMPTY_INPUT_SPLITS;
        }
        return conf.getInputFormat().getSplits(conf, numPartitions);
    }

    /*
     * The method below was modified to take care of the following
     * 1. when target files are not null, it generates a file aware input stream that validate
     * against the files
     * 2. if the data is binary, it returns a generic reader */
    public AsterixInputStream createInputStream(IHyracksTaskContext ctx) throws HyracksDataException {
        try {
            restoreConfig(ctx);
            return new HDFSInputStream(read, inputSplits, readSchedule, nodeName, conf, configuration);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    private void restoreConfig(IHyracksTaskContext ctx) throws HyracksDataException {
        if (!configured) {
            conf = confFactory.getConf();
            inputSplits = inputSplitsFactory.getSplits();
            nodeName = ctx.getJobletContext().getServiceContext().getNodeId();
            configured = true;
        }
    }

    /**
     * Get the cluster locations for this input stream factory. This method specifies on which asterix nodes the
     * external
     * adapter will run and how many threads per node.
     *
     * @return
     */
    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint() {
        clusterLocations = HDFSUtils.getPartitionConstraints((IApplicationContext) serviceCtx.getApplicationContext(),
                clusterLocations);
        return clusterLocations;
    }

    /**
     * This method initialize the scheduler which assigns responsibility of reading different logical input splits from
     * HDFS
     */
    private static void init(ICCServiceContext serviceCtx) throws HyracksDataException {
        if (!initialized) {
            synchronized (initLock) {
                if (!initialized) {
                    hdfsScheduler = HDFSUtils.initializeHDFSScheduler(serviceCtx);
                    initialized = true;
                }
            }
        }
    }

    @Override
    public DataSourceType getDataSourceType() {
        return ExternalDataUtils.getDataSourceType(configuration);
    }

    /**
     * HDFS Datasource is a special case in two ways:
     * 1. It supports indexing.
     * 2. It returns input as a set of writable object that we sometimes internally transform into a byte stream
     * Hence, it can produce:
     * 1. StreamRecordReader: When we transform the input into a byte stream.
     * 2. Indexing Stream Record Reader: When we transform the input into a byte stream and perform indexing.
     * 3. HDFS Record Reader: When we simply pass the Writable object as it is to the parser.
     */
    @SuppressWarnings("unchecked")
    @Override
    public IRecordReader<?> createRecordReader(IExternalDataRuntimeContext context) throws HyracksDataException {
        IHyracksTaskContext ctx = context.getTaskContext();
        try {
            if (recordReaderClazz != null) {
                StreamRecordReader streamReader = (StreamRecordReader) recordReaderClazz.getConstructor().newInstance();
                streamReader.configure(ctx, createInputStream(ctx), configuration);
                return streamReader;
            }
            restoreConfig(ctx);
            JobConf readerConf = conf;
            if (ctx.getWarningCollector().shouldWarn()
                    && configuration.get(ExternalDataConstants.KEY_INPUT_FORMAT.trim())
                            .equals(ExternalDataConstants.INPUT_FORMAT_PARQUET)) {
                /*
                 * JobConf is used to pass warnings from the ParquetReadSupport to ParquetReader. As multiple
                 * partitions can issue different warnings, we might have a race condition on JobConf. Thus, we
                 * should create a copy when warnings are enabled.
                 */
                readerConf = confFactory.getConf();
            }
            return createRecordReader(configuration, read, inputSplits, readSchedule, nodeName, readerConf, context);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public Class<?> getRecordClass() {
        return recordClass;
    }

    @Override
    public List<String> getRecordReaderNames() {
        return recordReaderNames;
    }

    @Override
    public IExternalDataRuntimeContext createExternalDataRuntimeContext(IHyracksTaskContext context, int partition) {
        IExternalFilterValueEmbedder valueEmbedder =
                filterEvaluatorFactory.createValueEmbedder(context.getWarningCollector());
        return new ExternalStreamRuntimeDataContext(context, partition, valueEmbedder);
    }

    private static IRecordReader<?> createRecordReader(Map<String, String> configuration, boolean[] read,
            InputSplit[] inputSplits, String[] readSchedule, String nodeName, JobConf conf,
            IExternalDataRuntimeContext context) {
        if (configuration.get(ExternalDataConstants.KEY_INPUT_FORMAT.trim())
                .equals(ExternalDataConstants.INPUT_FORMAT_PARQUET)) {
            return new ParquetFileRecordReader<>(read, inputSplits, readSchedule, nodeName, conf, context);
        } else {
            return new HDFSRecordReader<>(read, inputSplits, readSchedule, nodeName, conf);
        }
    }
}
