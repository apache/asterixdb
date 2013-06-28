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

package edu.uci.ics.pregelix.core.util;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * generate graph data from a base dataset
 */
@SuppressWarnings("deprecation")
public class DataGenerator {

    public static class MapMaxId extends MapReduceBase implements
            Mapper<LongWritable, Text, NullWritable, VLongWritable> {
        private NullWritable key = NullWritable.get();
        private VLongWritable value = new VLongWritable();

        @Override
        public void map(LongWritable id, Text inputValue, OutputCollector<NullWritable, VLongWritable> output,
                Reporter reporter) throws IOException {
            String[] vertices = inputValue.toString().split(" ");
            long max = Long.parseLong(vertices[0]);
            for (int i = 1; i < vertices.length; i++) {
                long vid = Long.parseLong(vertices[i]);
                if (vid > max)
                    max = vid;
            }
            value.set(max);
            output.collect(key, value);
        }
    }

    public static class ReduceMaxId extends MapReduceBase implements
            Reducer<NullWritable, VLongWritable, NullWritable, Text> {

        private NullWritable key = NullWritable.get();
        private long max = Long.MIN_VALUE;
        private OutputCollector<NullWritable, Text> output;

        @Override
        public void reduce(NullWritable inputKey, Iterator<VLongWritable> inputValue,
                OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException {
            while (inputValue.hasNext()) {
                long vid = inputValue.next().get();
                if (vid > max)
                    max = vid;
            }
            if (this.output == null)
                this.output = output;

        }

        @Override
        public void close() throws IOException {
            output.collect(key, new Text(new VLongWritable(max).toString()));
        }
    }

    public static class CombineMaxId extends MapReduceBase implements
            Reducer<NullWritable, VLongWritable, NullWritable, VLongWritable> {

        private NullWritable key = NullWritable.get();
        private long max = Long.MIN_VALUE;
        private OutputCollector<NullWritable, VLongWritable> output;

        @Override
        public void reduce(NullWritable inputKey, Iterator<VLongWritable> inputValue,
                OutputCollector<NullWritable, VLongWritable> output, Reporter reporter) throws IOException {
            while (inputValue.hasNext()) {
                long vid = inputValue.next().get();
                if (vid > max)
                    max = vid;
            }
            if (this.output == null)
                this.output = output;
        }

        public void close() throws IOException {
            output.collect(key, new VLongWritable(max));
        }
    }

    public static class MapRecordGen extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {

        private long maxId = 0;
        private Text text = new Text();
        private int x = 2;

        @Override
        public void configure(JobConf conf) {
            try {
                x = conf.getInt("hyracks.x", 2);
                String fileName = conf.get("hyracks.maxid.file");
                FileSystem dfs = FileSystem.get(conf);
                dfs.delete(new Path(fileName + "/_SUCCESS"), true);
                dfs.delete(new Path(fileName + "/_logs"), true);
                FileStatus[] files = dfs.listStatus(new Path(fileName));

                for (int i = 0; i < files.length; i++) {
                    if (!files[i].isDir()) {
                        DataInputStream input = dfs.open(files[i].getPath());
                        String id = input.readLine();
                        maxId = Long.parseLong(id) + 1;
                        input.close();
                    }
                }
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void map(LongWritable id, Text inputValue, OutputCollector<LongWritable, Text> output, Reporter reporter)
                throws IOException {
            String[] vertices = inputValue.toString().split(" ");

            /**
             * generate data x times
             */
            for (int k = 0; k < x; k++) {
                long max = maxId * k;
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < vertices.length - 1; i++) {
                    long vid = Long.parseLong(vertices[i]) + max;
                    sb.append(vid);
                    sb.append(" ");
                }
                long vid = Long.parseLong(vertices[vertices.length - 1]) + max;
                sb.append(vid);
                text.set(sb.toString().getBytes());
                output.collect(id, text);
            }
        }
    }

    public static class ReduceRecordGen extends MapReduceBase implements
            Reducer<LongWritable, Text, NullWritable, Text> {

        private NullWritable key = NullWritable.get();

        public void reduce(LongWritable inputKey, Iterator<Text> inputValue,
                OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException {
            while (inputValue.hasNext())
                output.collect(key, inputValue.next());
        }
    }

    public static void main(String[] args) throws IOException {

        JobConf job = new JobConf(DataGenerator.class);
        FileSystem dfs = FileSystem.get(job);
        String maxFile = "/maxtemp";
        dfs.delete(new Path(maxFile), true);

        job.setJobName(DataGenerator.class.getSimpleName() + "max ID");
        job.setMapperClass(MapMaxId.class);
        job.setCombinerClass(CombineMaxId.class);
        job.setReducerClass(ReduceMaxId.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(VLongWritable.class);

        job.setInputFormat(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(maxFile));
        job.setNumReduceTasks(1);
        JobClient.runJob(job);

        job = new JobConf(DataGenerator.class);
        job.set("hyracks.maxid.file", maxFile);
        job.setInt("hyracks.x", Integer.parseInt(args[2]));
        dfs.delete(new Path(args[1]), true);

        job.setJobName(DataGenerator.class.getSimpleName());
        job.setMapperClass(MapRecordGen.class);
        job.setReducerClass(ReduceRecordGen.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormat(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(Integer.parseInt(args[3]));

        if (args.length > 4) {
            if (args[4].startsWith("bzip"))
                FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
            if (args[4].startsWith("gz"))
                FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        }
        JobClient.runJob(job);
    }
}