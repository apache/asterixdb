/*
 * Copyright 2009-2010 by The Regents of the University of California
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

import org.apache.hadoop.io.Writable;

import edu.uci.ics.pregelix.api.util.ArrayListWritable;
import edu.uci.ics.pregelix.api.util.BspUtils;

/**
 * Wrapper around {@link ArrayListWritable} that allows the message class to be
 * set prior to calling readFields().
 * 
 * @param <M>
 *            message type
 */
public class MsgList<M extends Writable> extends ArrayListWritable<M> {
    /** Defining a layout version for a serializable class. */
    private static final long serialVersionUID = 100L;

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
}
