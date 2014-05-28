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
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

@SuppressWarnings("deprecation")
public class DuplicateGraph {
    public static class MapRecordOnly extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        static long largestId = 172655479;
        static long largestId2 = 172655479 * 2;
        static long largestId3 = 172655479 * 3;

        public void map(LongWritable id, Text inputValue, OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            StringTokenizer tokenizer = new StringTokenizer(inputValue.toString());
            String key = tokenizer.nextToken();
            long keyLong = Long.parseLong(key);
            String key2 = Long.toString(keyLong + largestId);
            String key3 = Long.toString(keyLong + largestId2);
            String key4 = Long.toString(keyLong + largestId3);

            StringBuilder value = new StringBuilder();
            StringBuilder value2 = new StringBuilder();
            StringBuilder value3 = new StringBuilder();
            StringBuilder value4 = new StringBuilder();
            while (tokenizer.hasMoreTokens()) {
                String neighbor = tokenizer.nextToken();
                long neighborLong = Long.parseLong(neighbor);
                value.append(neighbor + " ");
                value2.append((neighborLong + largestId) + " ");
                value3.append((neighborLong + largestId2) + " ");
                value4.append((neighborLong + largestId3) + " ");
            }
            output.collect(new Text(key), new Text(value.toString().trim()));
            output.collect(new Text(key2), new Text(value2.toString().trim()));
            output.collect(new Text(key3), new Text(value3.toString().trim()));
            output.collect(new Text(key4), new Text(value4.toString().trim()));
        }
    }

    public static void main(String[] args) throws IOException {
        JobConf job = new JobConf(DuplicateGraph.class);

        job.setJobName(DuplicateGraph.class.getSimpleName());
        job.setMapperClass(MapRecordOnly.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);

        job.setInputFormat(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(0);
        JobClient.runJob(job);
    }
}
