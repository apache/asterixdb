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
package edu.uci.ics.pregelix.example.aggregator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.api.graph.GlobalAggregator;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.example.io.DoubleWritable;
import edu.uci.ics.pregelix.example.io.VLongWritable;

/**
 * Test the case where the global aggregate's state is bloated
 * 
 * @author yingyib
 */
public class OverflowAggregator extends
        GlobalAggregator<VLongWritable, DoubleWritable, FloatWritable, DoubleWritable, Text, Text> {

    private int textLength = 0;
    private int inc = 32767;

    @Override
    public void init() {
        textLength = 0;
    }

    @Override
    public void step(Vertex<VLongWritable, DoubleWritable, FloatWritable, DoubleWritable> v)
            throws HyracksDataException {
        textLength += inc;
    }

    @Override
    public void step(Text partialResult) {
        textLength += partialResult.getLength();
    }

    @Override
    public Text finishPartial() {
        byte[] partialResult = new byte[textLength];
        for (int i = 0; i < partialResult.length; i++) {
            partialResult[i] = 'a';
        }
        Text text = new Text();
        text.set(partialResult);
        return text;
    }

    @Override
    public Text finishFinal() {
        byte[] result = new byte[textLength];
        for (int i = 0; i < result.length; i++) {
            result[i] = 'a';
        }
        Text text = new Text();
        text.set(result);
        return text;
    }

}
