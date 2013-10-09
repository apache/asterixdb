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
package edu.uci.ics.pregelix.example;

import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.example.io.VLongWritable;

/**
 * @author yingyib
 */
public class UpdateVertex extends Vertex<VLongWritable, Text, FloatWritable, VLongWritable> {

    @Override
    public void compute(Iterator<VLongWritable> msgIterator) throws Exception {
        if (getSuperstep() < 14) {
            Text value = getVertexValue();
            String newValue = value.toString() + value.toString();
            value.set(newValue);
            activate();
        } else if (getSuperstep() >= 14 && getSuperstep() < 28) {
            Text value = getVertexValue();
            String valueStr = value.toString();
            char[] valueChars = valueStr.toCharArray();
            if (valueChars.length <= 1) {
                throw new IllegalStateException("illegal value length: " + valueChars.length);
            }
            char[] newValueChars = new char[valueChars.length / 2];
            for (int i = 0; i < newValueChars.length; i++) {
                newValueChars[i] = valueChars[i];
            }
            value.set(new String(newValueChars));
            activate();
        } else {
            voteToHalt();
        }
    }
}
