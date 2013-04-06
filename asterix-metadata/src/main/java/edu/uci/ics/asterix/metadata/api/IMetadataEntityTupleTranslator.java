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

package edu.uci.ics.asterix.metadata.api;

import java.io.IOException;

import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Interface for translating the representation of metadata entities (datasets,
 * types, etc.) from their Java object representation to a serialized
 * representation in a Hyracks tuple, and vice versa. Implementations of this
 * interface are intended to be used within an IMetadataNode.
 */
public interface IMetadataEntityTupleTranslator<T> {

    /**
     * Transforms a metadata entity of type T from a given tuple to a Java
     * object (deserializing the appropriate field(s) in the tuple as
     * necessary).
     * 
     * @param tuple
     *            Tuple containing a serialized representation of a metadata
     *            entity of type T.
     * @return A new instance of a metadata entity of type T.
     * @throws MetadataException
     * @throws IOException
     */
    public T getMetadataEntytiFromTuple(ITupleReference tuple) throws MetadataException, IOException;

    /**
     * Serializes the given metadata entity of type T into an appropriate tuple
     * representation (i.e., some number of fields containing bytes).
     * 
     * @param metadataEntity
     *            Metadata entity to be written into a tuple.
     * @throws IOException
     */
    public ITupleReference getTupleFromMetadataEntity(T metadataEntity) throws MetadataException, IOException;
}