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
import java.util.Random;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.example.io.VLongWritable;

/**
 * @author yingyib
 */
public class UpdateVertex extends Vertex<VLongWritable, Text, FloatWritable, VLongWritable> {
    private final int MAX_VALUE_SIZE = 32768 / 2;
    private VLongWritable msg = new VLongWritable();
    private Text tempValue = new Text();
    private Random rand = new Random();

    @Override
    public void compute(Iterator<VLongWritable> msgIterator) throws Exception {
        if (getSuperstep() == 1) {
            updateAndSendMsg();
        } else if (getSuperstep() > 1 && getSuperstep() < 2) {
            verifyVertexSize(msgIterator);
            updateAndSendMsg();
        } else {
            voteToHalt();
        }
    }

    private void verifyVertexSize(Iterator<VLongWritable> msgIterator) {
        if (!msgIterator.hasNext()) {
            throw new IllegalStateException("no message for vertex " + " " + getVertexId() + " " + getVertexValue());
        }
        /**
         * verify the size
         */
        int valueSize = getVertexValue().toString().toCharArray().length;
        long expectedValueSize = msgIterator.next().get();
        if (valueSize != expectedValueSize) {
            if (valueSize == -expectedValueSize) {
                //verify fixed size update
                char[] valueCharArray = getVertexValue().toString().toCharArray();
                for (int i = 0; i < valueCharArray.length; i++) {
                    if (valueCharArray[i] != 'b') {
                        throw new IllegalStateException("vertex id: " + getVertexId()
                                + " has a un-propagated update in the last iteration");
                    }
                }
            } else {
                throw new IllegalStateException("vertex id: " + getVertexId() + " vertex value size:" + valueSize
                        + ", expected value size:" + expectedValueSize);
            }
        }
        if (msgIterator.hasNext()) {
            throw new IllegalStateException("more than one message for vertex " + " " + getVertexId() + " "
                    + getVertexValue());
        }
    }

    private void updateAndSendMsg() {
        int newValueSize = rand.nextInt(MAX_VALUE_SIZE);
        char[] charArray = new char[newValueSize];
        for (int i = 0; i < charArray.length; i++) {
            charArray[i] = 'a';
        }
        /**
         * set a self-message
         */
        msg.set(newValueSize);
        boolean fixedSize = getVertexId().get() < 2000;
        if (fixedSize) {
            int oldSize = getVertexValue().toString().toCharArray().length;
            charArray = new char[oldSize];
            for (int i = 0; i < oldSize; i++) {
                charArray[i] = 'b';
            }
            msg.set(-oldSize);
        }

        /**
         * set the vertex value
         */
        tempValue.set(new String(charArray));
        setVertexValue(tempValue);

        sendMsg(getVertexId(), msg);
        activate();
    }
}
