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
public class VertexAggregator {

    public static class MapRecordOnly extends MapReduceBase implements
            Mapper<LongWritable, Text, NullWritable, LongWritable> {
        private final NullWritable nullValue = NullWritable.get();
        private final LongWritable count = new LongWritable(1);

        public void map(LongWritable id, Text inputValue, OutputCollector<NullWritable, LongWritable> output,
                Reporter reporter) throws IOException {
            output.collect(nullValue, count);
        }
    }

    public static class CombineRecordOnly extends MapReduceBase implements
            Reducer<NullWritable, LongWritable, NullWritable, LongWritable> {
        private final NullWritable nullValue = NullWritable.get();

        public void reduce(NullWritable inputKey, Iterator<LongWritable> inputValue,
                OutputCollector<NullWritable, LongWritable> output, Reporter reporter) throws IOException {
            long count = 0;
            while (inputValue.hasNext())
                count += inputValue.next().get();
            output.collect(nullValue, new LongWritable(count));
        }
    }

    public static class ReduceRecordOnly extends MapReduceBase implements
            Reducer<NullWritable, LongWritable, NullWritable, Text> {
        private final NullWritable nullValue = NullWritable.get();

        public void reduce(NullWritable inputKey, Iterator<LongWritable> inputValue,
                OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException {
            long count = 0;
            while (inputValue.hasNext())
                count += inputValue.next().get();
            output.collect(nullValue, new Text(Long.toString(count)));
        }
    }

    public static void main(String[] args) throws IOException {
        JobConf job = new JobConf(VertexAggregator.class);

        job.setJobName(VertexAggregator.class.getSimpleName());
        job.setMapperClass(MapRecordOnly.class);
        job.setCombinerClass(CombineRecordOnly.class);
        job.setReducerClass(ReduceRecordOnly.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setInputFormat(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(Integer.parseInt(args[2]));
        JobClient.runJob(job);
    }
}
