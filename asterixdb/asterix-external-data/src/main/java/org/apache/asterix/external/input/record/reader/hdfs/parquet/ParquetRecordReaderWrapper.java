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
package org.apache.asterix.external.input.record.reader.hdfs.parquet;

import java.io.IOException;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.input.filter.ParquetFilterConverter;
import org.apache.asterix.external.input.filter.ParquetFilterExpression;
import org.apache.asterix.external.input.filter.embedder.IExternalFilterValueEmbedder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.util.LogRedactionUtil;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetRecordReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;

public class ParquetRecordReaderWrapper implements RecordReader<Void, VoidPointable> {
    private final ParquetRecordReader<IValueReference> realReader;
    private final long splitLen; // for getPos()

    private final VoidPointable valueContainer;

    private boolean firstRecord;
    private boolean eof;

    public ParquetRecordReaderWrapper(InputSplit oldSplit, JobConf oldJobConf, Reporter reporter,
            IExternalFilterValueEmbedder valueEmbedder, ParquetFilterExpression rowGroupFilter) throws IOException {
        splitLen = oldSplit.getLength();

        try {
            ReadSupport<IValueReference> readSupport = ParquetInputFormat.getReadSupportInstance(oldJobConf);
            ParquetReadSupport parquetReadSupport = (ParquetReadSupport) readSupport;
            parquetReadSupport.setValueEmbedder(valueEmbedder);
            JobConf readerConf = withRowGroupFilter(oldJobConf, oldSplit, rowGroupFilter);
            realReader = new ParquetRecordReader<>(readSupport, ParquetInputFormat.getFilter(readerConf));

            if (oldSplit instanceof MapredParquetInputFormat.ParquetInputSplitWrapper) {
                realReader.initialize(((MapredParquetInputFormat.ParquetInputSplitWrapper) oldSplit).realSplit,
                        readerConf, reporter);
            } else if (oldSplit instanceof FileSplit) {
                realReader.initialize((FileSplit) oldSplit, readerConf, reporter);
            } else {
                throw RuntimeDataException.create(ErrorCode.INVALID_PARQUET_FILE,
                        LogRedactionUtil.userData(oldSplit.toString()), "invalid file split");
            }

            // Set the path for value embedder
            valueEmbedder.setPath(getPath(oldSplit));

            valueContainer = new VoidPointable();
            firstRecord = false;
            eof = false;
            // read once to gain access to key and value objects
            if (realReader.nextKeyValue()) {
                firstRecord = true;
                valueContainer.set(realReader.getCurrentValue());
            } else {
                eof = true;
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        } catch (HyracksDataException | AsterixParquetRuntimeException e) {
            throw e;
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("not a Parquet file")) {
                throw RuntimeDataException.create(ErrorCode.INVALID_PARQUET_FILE,
                        LogRedactionUtil.userData(getPath(oldSplit)), "not a Parquet file");
            }

            throw RuntimeDataException.create(e);
        }
    }

    /**
     * Binds the row-group filter to this file and returns the configuration the reader should use.
     * <p>
     * The predicate has to be built here rather than when the query was compiled: it names a parquet-mr column
     * of a specific physical type, and only this file says what that type is. It also has to travel in the
     * configuration, because that is where {@code ParquetFileReader} looks for it when it decides which row
     * groups to read.
     * <p>
     * Only row-group elimination is wanted. Record-level and column-index filtering would re-decide which
     * individual rows survive, under parquet-mr's comparison and null semantics rather than the engine's, and
     * the engine evaluates the predicate itself regardless.
     *
     * @return a copy of {@code jobConf} carrying the predicate, or {@code jobConf} when nothing can be pushed
     */
    private static JobConf withRowGroupFilter(JobConf jobConf, InputSplit split, ParquetFilterExpression rowGroupFilter)
            throws IOException {
        if (rowGroupFilter == null) {
            return jobConf;
        }
        MessageType fileSchema;
        // the footer is read again when the reader opens the file; that second read is the price of knowing the
        // schema before the predicate has to exist, and is only paid when there is a filter to push
        try (ParquetFileReader reader =
                ParquetFileReader.open(HadoopInputFile.fromPath(new Path(getPath(split)), jobConf))) {
            fileSchema = reader.getFileMetaData().getSchema();
        }
        // read from the same configuration the reader is built from, so a literal cannot be pushed in a frame
        // or a precision the reader does not return
        FilterPredicate predicate = ParquetFilterConverter.toParquetFilter(fileSchema, rowGroupFilter,
                ParquetFilterConverter.ReadOptions.from(jobConf));
        if (predicate == null) {
            return jobConf;
        }
        JobConf withFilter = new JobConf(jobConf);
        ParquetInputFormat.setFilterPredicate(withFilter, predicate);
        withFilter.setBoolean(ParquetInputFormat.RECORD_FILTERING_ENABLED, false);
        withFilter.setBoolean(ParquetInputFormat.COLUMN_INDEX_FILTERING_ENABLED, false);
        return withFilter;
    }

    private static String getPath(InputSplit split) {
        if (split instanceof FileSplit) {
            return ((FileSplit) split).getPath().toString();
        } else if (split instanceof MapredParquetInputFormat.ParquetInputSplitWrapper) {
            return ((MapredParquetInputFormat.ParquetInputSplitWrapper) split).realSplit.getPath().toString();
        } else {
            return split.toString();
        }
    }

    @Override
    public void close() throws IOException {
        realReader.close();
    }

    @Override
    public Void createKey() {
        return null;
    }

    @Override
    public VoidPointable createValue() {
        return valueContainer;
    }

    @Override
    public long getPos() throws IOException {
        return (long) (splitLen * getProgress());
    }

    @Override
    public float getProgress() throws IOException {
        try {
            return realReader.getProgress();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean next(Void key, VoidPointable value) throws IOException {
        if (eof) {
            return false;
        }

        if (firstRecord) { // key & value are already read.
            firstRecord = false;
            value.set(valueContainer);
            return true;
        }

        try {
            if (realReader.nextKeyValue()) {
                if (value != null) {
                    value.set(realReader.getCurrentValue());
                }
                return true;
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        }

        eof = true; // strictly not required, just for consistency
        return false;
    }
}
