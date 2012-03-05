/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.external.data.adapter.api;

import java.nio.ByteBuffer;
import java.util.Map;

import edu.uci.ics.asterix.om.types.IAType;

public interface IDatasourceWriteAdapter extends IDatasourceAdapter {

    /**
     * Flushes tuples contained in the frame to the dataset stored in an
     * external data source. If required, the content of the frame is converted
     * into an appropriate format as required by the external data source.
     * 
     * @caller This method is invoked by the wrapping ASTERIX operator when data
     *         needs to be written to the external data source.
     * @param sourceAType
     *            The type associated with the data that is required to be
     *            written
     * @param frame
     *            the frame that needs to be flushed
     * @param datasourceSpecificParams
     *            A map containing other parameters that are specific to the
     *            target data source where data is to be written. For example
     *            when writing to a data source such as HDFS, an optional
     *            parameter is the replication factor.
     * @throws Exception
     */
    public void flush(IAType sourceAType, ByteBuffer frame, Map<String, String> datasourceSpecificParams)
            throws Exception;

}
