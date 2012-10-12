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

package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.io.Serializable;

/**
 * <code>InputSplit</code> represents the data to be processed by an individual {@link Mapper}.
 * <p>
 * Typically, it presents a byte-oriented view on the input and is the responsibility of {@link RecordReader} of the job to process this and present a record-oriented view.
 * 
 * @see InputFormat
 * @see RecordReader
 */
public abstract class InputSplit implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Get the size of the split, so that the input splits can be sorted by
     * size.
     * 
     * @return the number of bytes in the split
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract long getLength() throws IOException, InterruptedException;

    /**
     * Get the list of nodes by name where the data for the split would be
     * local. The locations do not need to be serialized.
     * 
     * @return a new array of the node nodes.
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract String[] getLocations() throws IOException, InterruptedException;
}