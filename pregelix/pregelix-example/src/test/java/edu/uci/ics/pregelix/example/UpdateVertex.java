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
    private final long MAX_VALUE_SIZE = 32768 / 2;
    private VLongWritable msg = new VLongWritable();
    private Text tempValue = new Text();

    @Override
    public void compute(Iterator<VLongWritable> msgIterator) throws Exception {
        if (getSuperstep() == 1) {
            updateAndSendMsg();
        } else if (getSuperstep() > 1 && getSuperstep() < 30) {
            verifyVertexSize(msgIterator);
            updateAndSendMsg();
        } else {
            voteToHalt();
        }
    }

    private void verifyVertexSize(Iterator<VLongWritable> msgIterator) {
        /**
         * verify the size
         */
        int valueSize = getVertexValue().getLength();
        long expectedValueSize = msgIterator.next().get();
        if (valueSize != expectedValueSize) {
            throw new IllegalStateException("verte value size:" + valueSize + ", expected value size:"
                    + expectedValueSize);
        }
    }

    private void updateAndSendMsg() {
        long newValueSize = (long) (Math.random()) % MAX_VALUE_SIZE;
        msg.set(newValueSize);
        char[] charArray = new char[(int) newValueSize];
        for (int i = 0; i < charArray.length; i++) {
            charArray[i] = 'a';
        }
        /**
         * set the vertex value
         */
        tempValue.set(new String(charArray));
        setVertexValue(tempValue);

        /**
         * send a self-message
         */
        msg.set(newValueSize);
        sendMsg(getVertexId(), msg);
        activate();
    }
}
