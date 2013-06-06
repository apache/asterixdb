/*
 * Copyright 2009-2013 by The Regents of the University of California
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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.hadoop.util.DatatypeHelper;
import edu.uci.ics.hyracks.dataflow.std.file.AbstractFileWriteOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IRecordWriter;

public class HadoopWriteOperatorDescriptor extends AbstractFileWriteOperatorDescriptor {

    private class HadoopFileWriter implements IRecordWriter {

        Object recordWriter;
        JobConf conf;
        Path finalOutputFile;
        Path tempOutputFile;
        Path tempDir;

        HadoopFileWriter(Object recordWriter, int index, JobConf conf) throws Exception {
            this.recordWriter = recordWriter;
            this.conf = conf;
            initialize(index, conf);
        }

        private void initialize(int index, JobConf conf) throws Exception {
            if (!(conf.getOutputFormat() instanceof NullOutputFormat)) {
                boolean isMap = conf.getNumReduceTasks() == 0;
                TaskAttemptID taskAttempId = new TaskAttemptID("0", index, isMap, index, index);
                conf.set("mapred.task.id", taskAttempId.toString());
                String suffix = new String("part-00000");
                suffix = new String(suffix.substring(0, suffix.length() - ("" + index).length()));
                suffix = suffix + index;
                outputPath = new Path(conf.get("mapred.output.dir"));
                tempDir = new Path(outputPath, FileOutputCommitter.TEMP_DIR_NAME);
                FileSystem fileSys = tempDir.getFileSystem(conf);
                if (!fileSys.mkdirs(tempDir)) {
                    throw new IOException("Mkdirs failed to create " + tempDir.toString());
                }
                tempOutputFile = new Path(tempDir, new Path("_" + taskAttempId.toString()));
                tempOutputFile = new Path(tempOutputFile, suffix);
                finalOutputFile = new Path(outputPath, suffix);
                if (conf.getUseNewMapper()) {
                    org.apache.hadoop.mapreduce.JobContext jobContext = new org.apache.hadoop.mapreduce.JobContext(
                            conf, null);
                    org.apache.hadoop.mapreduce.OutputFormat newOutputFormat = (org.apache.hadoop.mapreduce.OutputFormat) ReflectionUtils
                            .newInstance(jobContext.getOutputFormatClass(), conf);
                    recordWriter = newOutputFormat.getRecordWriter(new TaskAttemptContext(conf, taskAttempId));
                } else {
                    recordWriter = conf.getOutputFormat().getRecordWriter(FileSystem.get(conf), conf, suffix,
                            new Progressable() {
                                @Override
                                public void progress() {
                                }
                            });
                }
            }
        }

        @Override
        public void write(Object[] record) throws Exception {
            if (recordWriter != null) {
                if (conf.getUseNewMapper()) {
                    ((org.apache.hadoop.mapreduce.RecordWriter) recordWriter).write(record[0], record[1]);
                } else {
                    ((org.apache.hadoop.mapred.RecordWriter) recordWriter).write(record[0], record[1]);
                }
            }
        }

        @Override
        public void close() {
            try {
                if (recordWriter != null) {
                    if (conf.getUseNewMapper()) {
                        ((org.apache.hadoop.mapreduce.RecordWriter) recordWriter).close(new TaskAttemptContext(conf,
                                new TaskAttemptID()));
                    } else {
                        ((org.apache.hadoop.mapred.RecordWriter) recordWriter).close(null);
                    }
                    if (outputPath != null) {
                        FileSystem fileSystem = FileSystem.get(conf);
                        fileSystem.rename(tempOutputFile, finalOutputFile);
                        fileSystem.delete(tempDir, true);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static final long serialVersionUID = 1L;
    Map<String, String> jobConfMap;

    @Override
    protected IRecordWriter createRecordWriter(FileSplit fileSplit, int index) throws Exception {
        JobConf conf = DatatypeHelper.map2JobConf((HashMap) jobConfMap);
        conf.setClassLoader(this.getClass().getClassLoader());
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        FileSystem fileSystem = FileSystem.get(conf);
        Object recordWriter = null;
        return new HadoopFileWriter(recordWriter, index, conf);
    }

    Path outputPath;
    Path outputTempPath;

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
                Path path = new Path(fileSplit.getLocalFile().getFile().getPath());
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
        if (conf.getUseNewMapper()) {
            outputFormat = ReflectionUtils.newInstance(new org.apache.hadoop.mapreduce.JobContext(conf, null)
                    .getOutputFormatClass(), conf);
        } else {
            outputFormat = conf.getOutputFormat();
        }
        if (outputFormat instanceof NullOutputFormat) {
            FileSplit[] outputFileSplits = new FileSplit[numOutputters];
            for (int i = 0; i < numOutputters; i++) {
                String outputPath = "/tmp/" + System.currentTimeMillis() + i;
                outputFileSplits[i] = new FileSplit("localhost", new FileReference(new File(outputPath)));
            }
            return outputFileSplits;
        } else {

            FileSplit[] outputFileSplits = new FileSplit[numOutputters];
            String absolutePath = FileOutputFormat.getOutputPath(conf).toString();
            for (int index = 0; index < numOutputters; index++) {
                String suffix = new String("part-00000");
                suffix = new String(suffix.substring(0, suffix.length() - ("" + index).length()));
                suffix = suffix + index;
                String outputPath = absolutePath + "/" + suffix;
                outputFileSplits[index] = new FileSplit("localhost", outputPath);
            }
            return outputFileSplits;
        }
    }

    public HadoopWriteOperatorDescriptor(IOperatorDescriptorRegistry jobSpec, JobConf jobConf, int numMapTasks) throws Exception {
        super(jobSpec, getOutputSplits(jobConf, numMapTasks));
        this.jobConfMap = DatatypeHelper.jobConf2Map(jobConf);
        checkIfCanWriteToHDFS(super.splits);
    }
}
