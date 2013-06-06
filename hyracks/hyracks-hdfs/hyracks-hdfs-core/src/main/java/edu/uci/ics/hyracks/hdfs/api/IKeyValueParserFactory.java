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

import java.io.Serializable;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * Users need to implement this interface to use the HDFSReadOperatorDescriptor.
 * 
 * @param <K>
 *            the key type
 * @param <V>
 *            the value type
 */
public interface IKeyValueParserFactory<K, V> extends Serializable {

    /**
     * This method creates a key-value parser.
     * 
     * @param ctx
     *            the IHyracksTaskContext
     * @return a key-value parser instance.
     */
    public IKeyValueParser<K, V> createKeyValueParser(IHyracksTaskContext ctx) throws HyracksDataException;

}
