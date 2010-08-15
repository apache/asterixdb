/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.dataflow.hadoop;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.util.ReflectionUtils;

import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.hadoop.util.DatatypeHelper;
import edu.uci.ics.hyracks.dataflow.hadoop.util.HadoopFileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IRecordReader;

public class HadoopReadOperatorDescriptor extends AbstractHadoopFileScanOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private String inputFormatClassName;
    private Map<String, String> jobConfMap;

    private static class HDFSCustomReader implements IRecordReader {
        private RecordReader hadoopRecordReader;
        private Class inputKeyClass;
        private Class inputValueClass;
        private Object key;
        private Object value;

        public HDFSCustomReader(Map<String, String> jobConfMap, HadoopFileSplit inputSplit,
                String inputFormatClassName, Reporter reporter) {
            try {
                JobConf conf = DatatypeHelper.hashMap2JobConf((HashMap) jobConfMap);
                FileSystem fileSystem = null;
                try {
                    fileSystem = FileSystem.get(conf);
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }

                Class inputFormatClass = Class.forName(inputFormatClassName);
                InputFormat inputFormat = (InputFormat) ReflectionUtils.newInstance(inputFormatClass, conf);
                hadoopRecordReader = (RecordReader) inputFormat.getRecordReader(getFileSplit(inputSplit), conf,
                        reporter);
                if (hadoopRecordReader instanceof SequenceFileRecordReader) {
                    inputKeyClass = ((SequenceFileRecordReader) hadoopRecordReader).getKeyClass();
                    inputValueClass = ((SequenceFileRecordReader) hadoopRecordReader).getValueClass();
                } else {
                    inputKeyClass = hadoopRecordReader.createKey().getClass();
                    inputValueClass = hadoopRecordReader.createValue().getClass();
                }
                key = inputKeyClass.newInstance();
                value = inputValueClass.newInstance();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public Class getInputKeyClass() {
            return inputKeyClass;
        }

        public void setInputKeyClass(Class inputKeyClass) {
            this.inputKeyClass = inputKeyClass;
        }

        public Class getInputValueClass() {
            return inputValueClass;
        }

        public void setInputValueClass(Class inputValueClass) {
            this.inputValueClass = inputValueClass;
        }

        @Override
        public void close() {
            try {
                hadoopRecordReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public boolean read(Object[] record) throws Exception {
            if (!hadoopRecordReader.next(key, value)) {
                return false;
            }
            if (record.length == 1) {
                record[0] = value;
            } else {
                record[0] = key;
                record[1] = value;
            }
            return true;
        }

        private FileSplit getFileSplit(HadoopFileSplit hadoopFileSplit) {
            FileSplit fileSplit = new FileSplit(new Path(hadoopFileSplit.getFile()), hadoopFileSplit.getStart(),
                    hadoopFileSplit.getLength(), hadoopFileSplit.getHosts());
            return fileSplit;
        }
    }

    public HadoopReadOperatorDescriptor(Map<String, String> jobConfMap, JobSpecification spec,
            HadoopFileSplit[] splits, String inputFormatClassName, RecordDescriptor recordDescriptor) {
        super(spec, splits, recordDescriptor);
        this.inputFormatClassName = inputFormatClassName;
        this.jobConfMap = jobConfMap;
    }

    public HadoopReadOperatorDescriptor(Map<String, String> jobConfMap, InetSocketAddress nameNode,
            JobSpecification spec, String inputFormatClassName, RecordDescriptor recordDescriptor) {
        super(spec, null, recordDescriptor);
        this.inputFormatClassName = inputFormatClassName;
        this.jobConfMap = jobConfMap;
    }

    @Override
    protected IRecordReader createRecordReader(HadoopFileSplit fileSplit, RecordDescriptor desc) throws Exception {
        Reporter reporter = createReporter();
        IRecordReader recordReader = new HDFSCustomReader(jobConfMap, fileSplit, inputFormatClassName, reporter);
        return recordReader;
    }
}