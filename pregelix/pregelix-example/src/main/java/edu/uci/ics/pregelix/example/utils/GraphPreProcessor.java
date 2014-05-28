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

@SuppressWarnings("deprecation")
public class GraphPreProcessor {
    public static class MapRecordOnly extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable id, Text inputValue, OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            StringTokenizer tokenizer = new StringTokenizer(inputValue.toString());
            String key = tokenizer.nextToken();
            //skip the old key
            tokenizer.nextToken();

            StringBuilder value = new StringBuilder();
            while (tokenizer.hasMoreTokens()) {
                value.append(tokenizer.nextToken() + " ");
            }
            output.collect(new Text(key), new Text(value.toString().trim()));
        }
    }

    public static void main(String[] args) throws IOException {
        JobConf job = new JobConf(GraphPreProcessor.class);

        job.setJobName(GraphPreProcessor.class.getSimpleName());
        job.setMapperClass(MapRecordOnly.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormat(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(0);
        JobClient.runJob(job);
    }
}
