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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.hadoop.util.DatatypeHelper;
import edu.uci.ics.hyracks.dataflow.std.file.AbstractFileWriteOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IRecordWriter;
import edu.uci.ics.hyracks.dataflow.std.file.RecordWriter;

public class HadoopWriteOperatorDescriptor extends AbstractFileWriteOperatorDescriptor {

    private static String nullWritableClassName = NullWritable.class.getName();

    private static class HadoopFileWriter implements IRecordWriter {

        Object recordWriter;
        JobConf conf;
        final boolean useNewMapReduceLib;
        HadoopFileWriter(Object recordWriter,JobConf conf) {
            this.recordWriter = recordWriter;
            this.conf = conf;
            useNewMapReduceLib = conf.getUseNewMapper();
        }

        @Override
        public void write(Object[] record) throws Exception {
            if (useNewMapReduceLib){
                ((org.apache.hadoop.mapreduce.RecordWriter)recordWriter).write(record[0], record[1]);
            } else {
                ((org.apache.hadoop.mapred.RecordWriter)recordWriter).write(record[0], record[1]);
            }    
        }

        @Override
        public void close() {
            try {
                if (useNewMapReduceLib){
                   ((org.apache.hadoop.mapreduce.RecordWriter)recordWriter).close(new TaskAttemptContext(conf, new TaskAttemptID()));
                } else {
                    ((org.apache.hadoop.mapred.RecordWriter)recordWriter).close(null);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class HadoopSequenceWriter implements IRecordWriter {
        private Writer writer;

        HadoopSequenceWriter(Writer writer) throws Exception {
            this.writer = writer;
        }

        @Override
        public void close() {
            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void write(Object[] record) throws Exception {
            Object key = record[0];
            Object value = record[1];
            writer.append(key, value);
        }
    }

    private static final long serialVersionUID = 1L;
    Map<String, String> jobConfMap;

    @Override
    protected IRecordWriter createRecordWriter(FileSplit fileSplit, int index) throws Exception {
        JobConf conf = DatatypeHelper.map2JobConf((HashMap) jobConfMap);
        conf.setClassLoader(this.getClass().getClassLoader());
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(conf);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        Path path = new Path(fileSplit.getPath());
        checkIfCanWriteToHDFS(new FileSplit[] { fileSplit });
        Object recordWriter  = null;
        boolean sequenceFileOutput = false;
        Object outputFormat = null;
        if(conf.getUseNewMapper()){
            org.apache.hadoop.mapreduce.OutputFormat newOutputFormat = (org.apache.hadoop.mapreduce.OutputFormat)ReflectionUtils.newInstance((new JobContext(conf,null)).getOutputFormatClass(),conf);
            if(newOutputFormat instanceof org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat) {
                 sequenceFileOutput = true;
            }else{
                recordWriter = newOutputFormat.getRecordWriter(new TaskAttemptContext(conf, new TaskAttemptID()));
            }    
        }else {
            if(conf.getOutputFormat() instanceof SequenceFileOutputFormat) {
                sequenceFileOutput = true;
            } else {
                recordWriter = conf.getOutputFormat().getRecordWriter(fileSystem, conf, "" + System.currentTimeMillis(), new Progressable() {
                @Override
                public void progress() {}
                });
            }
        }
        if(!sequenceFileOutput) {
            return new HadoopFileWriter(recordWriter,conf);
        } else { 
            Class keyClass = Class.forName(conf.getOutputKeyClass().getName());
            Class valueClass = Class.forName(conf.getOutputValueClass().getName());
            conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
            Writer writer = SequenceFile.createWriter(fileSystem, conf, path, keyClass, valueClass);
            return new HadoopSequenceWriter(writer);    
        }   
    }
    


    protected Reporter createReporter() {
    return new Reporter() {
        @Override
        public Counter getCounter(Enum<?> name) {
            return null;
        }

        @Override
        public Counter getCounter(String group, String name) {
            return null;
        }

        @Override
        public InputSplit getInputSplit() throws UnsupportedOperationException {
            return null;
        }

        @Override
        public void incrCounter(Enum<?> key, long amount) {

        }

        @Override
        public void incrCounter(String group, String counter, long amount) {

        }

        @Override
        public void progress() {

        }

        @Override
        public void setStatus(String status) {

        }
    };
}

    private boolean checkIfCanWriteToHDFS(FileSplit[] fileSplits) throws Exception {
        JobConf conf = DatatypeHelper.map2JobConf((HashMap) jobConfMap);
        try {
            FileSystem fileSystem = FileSystem.get(conf);
            for (FileSplit fileSplit : fileSplits) {
                Path path = new Path(fileSplit.getPath());
                if (fileSystem.exists(path)) {
                    throw new Exception(" Output path :  already exists : " + path);
                }
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
            throw ioe;
        }
        return true;
    }

    private static FileSplit[] getOutputSplits(JobConf conf, int noOfMappers) throws ClassNotFoundException {
        int numOutputters = conf.getNumReduceTasks() != 0 ? conf.getNumReduceTasks() : noOfMappers;
        Object outputFormat = null;
        if(conf.getUseNewMapper()) {
            outputFormat = ReflectionUtils.newInstance(new JobContext(conf,null).getOutputFormatClass(), conf);
        } else {
            outputFormat = conf.getOutputFormat();
        }
        if (outputFormat instanceof NullOutputFormat) {
            FileSplit[] outputFileSplits = new FileSplit[numOutputters];
            for (int i = 0; i < numOutputters; i++) {
                String outputPath = "/tmp/" + System.currentTimeMillis() + i;
                outputFileSplits[i] = new FileSplit("localhost", new File(outputPath));
            }
            return outputFileSplits;
        } else {

            FileSplit[] outputFileSplits = new FileSplit[numOutputters];
            String absolutePath = FileOutputFormat.getOutputPath(conf).toString();
            System.out.println("absolute path:" + absolutePath);
            for (int index = 0; index < numOutputters; index++) {
                String suffix = new String("part-00000");
                suffix = new String(suffix.substring(0, suffix.length() - ("" + index).length()));
                suffix = suffix + index;
                String outputPath = absolutePath + "/" + suffix;
                System.out.println("output path :" + outputPath);
                outputFileSplits[index] = new FileSplit("localhost", outputPath);
            }
            return outputFileSplits;
        }

    }

    public HadoopWriteOperatorDescriptor(JobSpecification jobSpec, JobConf jobConf, int numMapTasks) throws Exception {
        super(jobSpec, getOutputSplits(jobConf, numMapTasks));
        this.jobConfMap = DatatypeHelper.jobConf2Map(jobConf);
        checkIfCanWriteToHDFS(super.splits);
    }
}
