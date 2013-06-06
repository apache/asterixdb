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

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
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

@SuppressWarnings("deprecation")
public class DataBalancer {

    public static class MapRecordOnly extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {

        public void map(LongWritable id, Text inputValue, OutputCollector<LongWritable, Text> output, Reporter reporter)
                throws IOException {
            output.collect(id, inputValue);
        }
    }

    public static class ReduceRecordOnly extends MapReduceBase implements
            Reducer<LongWritable, Text, NullWritable, Text> {

        NullWritable key = NullWritable.get();

        public void reduce(LongWritable inputKey, Iterator<Text> inputValue,
                OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException {
            while (inputValue.hasNext())
                output.collect(key, inputValue.next());
        }
    }

    public static void main(String[] args) throws IOException {
        JobConf job = new JobConf(DataBalancer.class);

        job.setJobName(DataBalancer.class.getSimpleName());
        job.setMapperClass(MapRecordOnly.class);
        job.setReducerClass(ReduceRecordOnly.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormat(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(Integer.parseInt(args[2]));

        if (args.length > 3) {
            if (args[3].startsWith("bzip"))
                FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
            if (args[3].startsWith("gz"))
                FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        }
        JobClient.runJob(job);
    }
}