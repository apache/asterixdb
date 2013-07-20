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

package edu.uci.ics.pregelix.api.graph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.pregelix.api.io.WritableSizable;
import edu.uci.ics.pregelix.api.util.ArrayListWritable;
import edu.uci.ics.pregelix.api.util.BspUtils;

/**
 * Wrapper around {@link ArrayListWritable} that allows the message class to be
 * set prior to calling readFields().
 * 
 * @param <M>
 *            message type
 */
public class MsgList<M extends WritableSizable> extends ArrayListWritable<M> {
    /** Defining a layout version for a serializable class. */
    private static final long serialVersionUID = 1L;
    private byte start = 1;
    private byte end = 2;

    /**
     * Default constructor.s
     */
    public MsgList() {
        super();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setClass() {
        setClass((Class<M>) BspUtils.getMessageValueClass(getConf()));
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeByte(start | end);
        super.write(output);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        byte startEnd = input.readByte();
        this.start = (byte) (startEnd & 1);
        this.end = (byte) (startEnd & 2);
        super.readFields(input);
    }

    public final void setSegmentStart(boolean segStart) {
        this.start = (byte) (segStart ? 1 : 0);
    }

    public final void setSegmentEnd(boolean segEnd) {
        this.end = (byte) (segEnd ? 2 : 0);
    }

    public boolean segmentStart() {
        return start == 1 ? true : false;
    }

    public boolean segmentEnd() {
        return end == 2 ? true : false;
    }
}
