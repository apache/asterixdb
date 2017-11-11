/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.metadata.api;

import java.io.Serializable;
import java.rmi.RemoteException;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Interface for translating the representation of metadata entities (datasets,
 * types, etc.) from their Java object representation to a serialized
 * representation in a Hyracks tuple, and vice versa. Implementations of this
 * interface are intended to be used within an IMetadataNode.
 */
public interface IMetadataEntityTupleTranslator<T> extends Serializable {

    /**
     * Transforms a metadata entity of type T from a given tuple to a Java object
     * (deserializing the appropriate field(s) in the tuple as necessary).
     *
     * @param tuple
     *            Tuple containing a serialized representation of a metadata entity
     *            of type T.
     * @return A new instance of a metadata entity of type T.
     * @throws AlgebricksException
     * @throws HyracksDataException
     * @throws RemoteException
     */
    T getMetadataEntityFromTuple(ITupleReference tuple)
            throws AlgebricksException, HyracksDataException, RemoteException;

    /**
     * Serializes the given metadata entity of type T into an appropriate tuple
     * representation (i.e., some number of fields containing bytes).
     *
     * @param metadataEntity
     *            Metadata entity to be written into a tuple.
     * @throws AlgebricksException
     * @throws HyracksDataException
     */
    ITupleReference getTupleFromMetadataEntity(T metadataEntity) throws AlgebricksException, HyracksDataException;
}
