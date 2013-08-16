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

package edu.uci.ics.hyracks.hdfs.api;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * Users need to implement this interface to use the HDFSReadOperatorDescriptor.
 * 
 * @param <K>
 *            the key type
 * @param <V>
 *            the value type
 */
public interface IKeyValueParser<K, V> {

    /**
     * Initialize the key value parser.
     * 
     * @param writer
     *            The hyracks writer for outputting data.
     * @throws HyracksDataException
     */
    public void open(IFrameWriter writer) throws HyracksDataException;

    /**
     * @param key
     * @param value
     * @param writer
     * @param fileName
     * @throws HyracksDataException
     */
    public void parse(K key, V value, IFrameWriter writer, String fileString) throws HyracksDataException;

    /**
     * Flush the residual tuples in the internal buffer to the writer.
     * This method is called in the close() of HDFSReadOperatorDescriptor.
     * 
     * @param writer
     *            The hyracks writer for outputting data.
     * @throws HyracksDataException
     */
    public void close(IFrameWriter writer) throws HyracksDataException;
}
