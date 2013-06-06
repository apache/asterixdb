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

package edu.uci.ics.pregelix.core.hadoop.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import edu.uci.ics.pregelix.api.util.BspUtils;

public class Message<I extends Writable, M extends Writable> implements Writable {
    private I receiverId;
    private M body;
    private Configuration conf;

    public Message() {
    }

    public Message(I receiverId, M body) {
        this.receiverId = receiverId;
        this.body = body;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getConf() {
        return conf;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readFields(DataInput input) throws IOException {
        if (this.receiverId == null && this.body == null) {
            setClass((Class<I>) BspUtils.getVertexIndexClass(getConf()),
                    (Class<M>) BspUtils.getMessageValueClass(getConf()));
        }
        receiverId.readFields(input);
        body.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        receiverId.write(output);
        body.write(output);
    }

    public I getReceiverVertexId() {
        return receiverId;
    }

    public M getMessageBody() {
        return body;
    }

    private void setClass(Class<I> idClass, Class<M> bodyClass) {
        receiverId = ReflectionUtils.newInstance(idClass, getConf());
        body = ReflectionUtils.newInstance(bodyClass, getConf());
    }

}
