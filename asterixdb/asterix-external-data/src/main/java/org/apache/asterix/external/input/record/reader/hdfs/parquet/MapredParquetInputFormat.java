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

import static java.util.Arrays.asList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.asterix.external.input.filter.embedder.IExternalFilterValueEmbedder;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetInputSplit;

/**
 * For the Original implementation, see {@code DeprecatedParquetInputFormat}
 * The original implementation has been modified to have {@code VoidPointable}
 * instead of {@code org.apache.parquet.hadoop.mapred.Container}
 * <p>
 * AsterixDB currently support the older Hadoop API (@see org.apache.hadoop.mapred).
 * The newer API (@see org.apache.hadoop.mapreduce) is not yet supported.
 * Beware before upgrading Apache Parquet version.
 */
public class MapredParquetInputFormat extends org.apache.hadoop.mapred.FileInputFormat<Void, VoidPointable> {

    private final ParquetInputFormat<ArrayBackedValueStorage> realInputFormat = new ParquetInputFormat<>();
    private IExternalFilterValueEmbedder valueEmbedder;

    @Override
    public RecordReader<Void, VoidPointable> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
            throws IOException {
        return new ParquetRecordReaderWrapper(split, job, reporter, valueEmbedder);
    }

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        if (isTaskSideMetaData(job)) {
            return super.getSplits(job, numSplits);
        }

        List<Footer> footers = getFooters(job);
        List<ParquetInputSplit> splits = realInputFormat.getSplits(job, footers);
        if (splits == null) {
            return null; //NOSONAR
        }
        InputSplit[] resultSplits = new InputSplit[splits.size()];
        int i = 0;
        for (ParquetInputSplit split : splits) {
            resultSplits[i++] = new ParquetInputSplitWrapper(split);
        }
        return resultSplits;
    }

    public List<Footer> getFooters(JobConf job) throws IOException {
        return realInputFormat.getFooters(job, asList(super.listStatus(job)));
    }

    public void setValueEmbedder(IExternalFilterValueEmbedder valueEmbedder) {
        this.valueEmbedder = valueEmbedder;
    }

    public static boolean isTaskSideMetaData(JobConf job) {
        return job.getBoolean(ParquetInputFormat.TASK_SIDE_METADATA, true);
    }

    static class ParquetInputSplitWrapper implements InputSplit {

        ParquetInputSplit realSplit;

        @SuppressWarnings("unused") // MapReduce instantiates this.
        public ParquetInputSplitWrapper() {
        }

        public ParquetInputSplitWrapper(ParquetInputSplit realSplit) {
            this.realSplit = realSplit;
        }

        @Override
        public long getLength() throws IOException {
            return realSplit.getLength();
        }

        @Override
        public String[] getLocations() throws IOException {
            return realSplit.getLocations();
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            realSplit = new ParquetInputSplit();
            realSplit.readFields(in);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            realSplit.write(out);
        }

        @Override
        public String toString() {
            return realSplit.toString();
        }
    }
}
