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

package edu.uci.ics.pregelix.example.utils;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
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

@SuppressWarnings("deprecation")
public class FindLargest {
    public static class MapRecordOnly extends MapReduceBase implements
            Mapper<LongWritable, Text, LongWritable, NullWritable> {

        public void map(LongWritable id, Text inputValue, OutputCollector<LongWritable, NullWritable> output,
                Reporter reporter) throws IOException {
            StringTokenizer tokenizer = new StringTokenizer(inputValue.toString());
            String key = tokenizer.nextToken();
            output.collect(new LongWritable(Long.parseLong(key)), NullWritable.get());
        }
    }

    public static class ReduceRecordOnly extends MapReduceBase implements
            Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {

        NullWritable value = NullWritable.get();
        long currentMax = Long.MIN_VALUE;
        OutputCollector<LongWritable, NullWritable> output;

        public void reduce(LongWritable inputKey, Iterator<NullWritable> inputValue,
                OutputCollector<LongWritable, NullWritable> output, Reporter reporter) throws IOException {
            if (this.output == null) {
                this.output = output;
            }
            if (inputKey.get() > currentMax) {
                currentMax = inputKey.get();
            }
        }

        @Override
        public void close() throws IOException {
            output.collect(new LongWritable(currentMax), value);
        }
    }

    public static void main(String[] args) throws IOException {
        JobConf job = new JobConf(GraphPreProcessor.class);

        job.setJobName(GraphPreProcessor.class.getSimpleName());
        job.setMapperClass(MapRecordOnly.class);
        job.setReducerClass(ReduceRecordOnly.class);
        job.setCombinerClass(ReduceRecordOnly.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setInputFormat(TextInputFormat.class);
        for (int i = 0; i < args.length - 2; i++) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 2]));
        job.setNumReduceTasks(Integer.parseInt(args[args.length - 1]));
        JobClient.runJob(job);
    }
}
