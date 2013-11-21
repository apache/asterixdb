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
package edu.uci.ics.asterix.metadata.feeds;

import java.io.Serializable;

import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

/**
 * Base interface for IGenericDatasetAdapterFactory and ITypedDatasetAdapterFactory.
 * Acts as a marker interface indicating that the implementation provides functionality
 * for creating an adapter.
 */
public interface IAdapterFactory extends Serializable {

    /**
     * A 'GENERIC' adapter can be configured to return a given datatype.
     * A 'TYPED' adapter returns records with a pre-defined datatype.
     */
    public enum AdapterType {
        GENERIC,
        TYPED
    }

    public enum SupportedOperation {
        READ,
        WRITE,
        READ_WRITE
    }

    /**
     * Returns the type of adapter indicating if the adapter can be used for
     * reading from an external data source or writing to an external data
     * source or can be used for both purposes.
     * 
     * @see SupportedOperation
     * @return
     */
    public SupportedOperation getSupportedOperations();

    /**
     * Returns the display name corresponding to the Adapter type that is created by the factory.
     * 
     * @return the display name
     */
    public String getName();

    /**
     * Returns the type of the adapter (GENERIC or TYPED)
     * 
     * @return
     */
    public AdapterType getAdapterType();

    /**
     * Returns a list of partition constraints. A partition constraint can be a
     * requirement to execute at a particular location or could be cardinality
     * constraints indicating the number of instances that need to run in
     * parallel. example, a IDatasourceAdapter implementation written for data
     * residing on the local file system of a node cannot run on any other node
     * and thus has a location partition constraint. The location partition
     * constraint can be expressed as a node IP address or a node controller id.
     * In the former case, the IP address is translated to a node controller id
     * running on the node with the given IP address.
     */
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception;

    /**
     * Creates an instance of IDatasourceAdapter.
     * 
     * @param HyracksTaskContext
     * @param partition
     * @return An instance of IDatasourceAdapter.
     * @throws Exception
     */
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws Exception;

}
